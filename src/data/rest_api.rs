use serde::Deserialize;
use serde::Serialize;
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
    pub ingestion_id: String,
}

#[derive(Debug, Serialize, Clone, Deserialize, Default)]
pub struct TraversalNodeRequest {
    pub direction: String,
    pub relation_type: Option<String>,
    pub max_depth: usize,
}
