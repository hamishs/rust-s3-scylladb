use crate::data::model::{Node, Relation, TraversalNode, DIR, REL};
use crate::db::model::{DbNode, DbNodeSimple};
use crate::db::scylladb::ScyllaDbService;
use actix_web::error::ErrorInternalServerError;
use actix_web::web::Json;
use actix_web::{get, post, web, web::Data, Error, HttpResponse};
use color_eyre::Result;
use futures::future::{BoxFuture, FutureExt};
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tracing::info;
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
pub struct IngestionRequest {
    pub ingestion_id: String,
    pub files: Vec<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(transparent)]
pub struct File {
    pub files: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct AppError {
    message: String,
}

#[derive(Debug, Serialize, Clone, Deserialize, Default)]
pub struct GetNodeRequest {
    pub get_tags: Option<bool>,
    pub get_relations: Option<bool>,
}

#[derive(Debug, Serialize, Clone, Deserialize, Default)]
pub struct PostNodeRequest {
    pub uuid: Uuid,
    pub name: String,
    pub node_type: String,
    pub url: String,
    pub job_id: String,
}

#[derive(Debug, Serialize, Clone, Deserialize, Default)]
pub struct PostSuccessorRequest {
    pub uuid: String,
    pub name: String,
    pub job_id: String,
}

#[derive(Debug, Serialize, Clone, Deserialize, Default)]
pub struct TraversalNodeRequest {
    pub direction: String,
    pub relation_type: Option<String>,
    pub max_depth: usize,
}

pub struct AppState {
    pub db_svc: ScyllaDbService,
    pub semaphore: Arc<Semaphore>,
    pub region: String,
}

#[get("/node/{id}")]
async fn get_by_id(
    path: web::Path<String>,
    query_data: web::Query<GetNodeRequest>,
    state: Data<AppState>,
) -> Result<HttpResponse, Error> {
    let now = Instant::now();
    let id = path.into_inner();
    info!("get_by_id {}, relations? {:?}", id, query_data);

    let relations = query_data.get_relations.unwrap_or_default();
    let tags = query_data.get_tags.unwrap_or(true);

    let ret = get_node(&state.db_svc, &id, tags, relations).await?;

    let elapsed = now.elapsed();
    info!("get_by_id time: {:.2?}", elapsed);
    Ok(HttpResponse::Ok().json(ret))
}

#[get("/traversal/{id}")]
async fn traversal_by_id(
    path: web::Path<String>,
    query_data: web::Query<TraversalNodeRequest>,
    state: Data<AppState>,
) -> Result<HttpResponse, Error> {
    let now = Instant::now();
    let id = path.into_inner();
    info!("traversal_by_id: {}", id);

    let result: Option<TraversalNode> = traversal_recur(
        state,
        id,
        Arc::new(query_data.direction.clone()),
        Arc::new(query_data.relation_type.clone()),
        0,
        query_data.max_depth,
    )
    .await;

    let elapsed = now.elapsed();
    info!("traversal time: {:.2?}", elapsed);
    Ok(HttpResponse::Ok().json(result))
}

fn traversal_recur<'a>(
    state: Data<AppState>,
    id: String,
    direction: Arc<String>,
    relation_type: Arc<Option<String>>,
    depth: usize,
    max: usize,
) -> BoxFuture<'a, Option<TraversalNode>> {
    async move {
        let db_nodes = state
            .db_svc
            .get_node_traversal(&id, &direction, &relation_type)
            .await
            .ok()?;
        let mut node = TraversalNode::from(db_nodes, depth)?;

        if depth < max && node.relation_ids.len() > 0 {
            let mut handlers: Vec<JoinHandle<_>> = Vec::new();

            for id in &node.relation_ids {
                handlers.push(tokio::spawn(traversal_recur(
                    state.clone(),
                    id.to_string(),
                    direction.clone(),
                    relation_type.clone(),
                    depth + 1,
                    max,
                )));
            }

            for thread in handlers {
                let child = thread.await.ok()?;
                node.relations.push(child?);
            }
        }

        Some(node)
    }
    .boxed()
}

async fn get_node(
    db: &ScyllaDbService,
    id: &str,
    tags: bool,
    relations: bool,
) -> Result<Json<Option<Node>>, Error> {
    let db_nodes = db
        .get_node(id, tags, relations)
        .await
        .map_err(ErrorInternalServerError)?;

    let node = Node::from(db_nodes);

    Ok(web::Json(node))
}

#[post("/node")]
async fn create_node(
    payload: web::Json<PostNodeRequest>,
    state: Data<AppState>,
) -> Result<HttpResponse, Error> {
    let now = Instant::now();
    info!("create_node: {:?}", payload);

    // create the node from the payload data
    let node = DbNodeSimple {
        uuid: payload.uuid,
        name: payload.name.clone(),
        node_type: payload.node_type.clone(),
        url: payload.url.clone(),
        job_id: payload.job_id.clone(),
    };
    let db_node = DbNode::from_simple(node);

    let node = Node::new(
        db_node.uuid,
        db_node.job_id.clone(),
        db_node.url.clone(),
        db_node.name.clone(),
        db_node.node_type.clone(),
        vec![],
    );

    let result = state.db_svc.save_nodes(vec![db_node]).await;

    let elapsed = now.elapsed();
    info!("create_node time: {:.2?}", elapsed);
    match result {
        Ok(_) => Ok(HttpResponse::Ok().json(node)),
        Err(e) => Err(ErrorInternalServerError(e)),
    }
}

#[post("/node/{id}/successor")]
async fn add_successor(
    path: web::Path<String>,
    payload: web::Json<PostSuccessorRequest>,
    state: Data<AppState>,
) -> Result<HttpResponse, Error> {
    let now = Instant::now();
    let id: String = path.into_inner();
    info!("add_successor {}", id);

    // UUID from String
    let uuid = Uuid::parse_str(&id).unwrap();

    // create the new edge from the payload data
    let edge: DbNode = DbNode::relation(
        uuid,
        payload.job_id.clone(),
        DIR::OUT.to_string(),
        REL::ISPARENT.to_string(),
        payload.uuid.clone(),
        payload.name.clone(),
    );

    let relation = Relation::from(
        payload.name.clone(),
        REL::ISPARENT.to_string(),
        payload.uuid.clone(),
        true,
    );

    let result = state.db_svc.save_nodes(vec![edge]).await;

    let elapsed = now.elapsed();
    info!("add_successor time: {:.2?}", elapsed);
    match result {
        Ok(_) => Ok(HttpResponse::Ok().json(relation)),
        Err(e) => Err(ErrorInternalServerError(e)),
    }
}
