use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

use strum_macros::Display;

use crate::db::model::DbNode;
use crate::db::model::DbRelation;

// UUID struct
const NAMESPACE_UUID: Uuid = Uuid::from_bytes([
    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
]);

#[derive(Display, Debug)]
pub enum DIR {
    IN,
    OUT,
}

#[derive(Display, Debug)]
pub enum REL {
    ISPARENT,
    ISCHILD,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    pub uuid: Uuid,
    pub job_id: String,
    pub name: String,
    pub url: String,
    #[serde(rename = "type")]
    pub node_type: String,
    pub tags: Vec<(String, String)>,
    pub relations: Vec<Relation>,
}

impl Node {
    pub fn new(
        id: Uuid,
        job_id: String,
        url: String,
        name: String,
        node_type: String,
        attrs: Vec<(String, String)>,
    ) -> Self {
        Self {
            job_id,
            uuid: id,
            url,
            name,
            node_type,
            tags: attrs,
            relations: vec![],
        }
    }

    pub fn from(db_entries: Vec<DbNode>) -> Option<Node> {
        let mut n = db_entries.get(0)?;
        let empty = vec![];
        let attrs = n.tags.as_ref().unwrap_or(&empty);
        let mut node = Node::new(
            n.uuid,
            n.job_id.clone(),
            n.url.clone(),
            n.name.clone(),
            n.node_type.clone(),
            attrs.clone(),
        );

        let mut relations = vec![];
        for i in 0..db_entries.len() {
            if i == 0 {
                continue;
            } else {
                n = db_entries.get(i)?;
                let outbound = n.direction.clone().unwrap() == DIR::OUT.to_string();
                let r = Relation::from(
                    n.name.clone(),
                    n.relation.clone().unwrap(),
                    n.relates_to.clone().unwrap(),
                    outbound,
                );
                relations.push(r);
            }
        }

        node.relations = relations;

        Some(node)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraversalNode {
    pub uuid: Uuid,
    pub depth: usize,
    pub name: String,
    #[serde(rename = "type")]
    pub node_type: String,
    pub relations: Vec<TraversalNode>,
    pub relation_ids: Vec<String>,
}

impl TraversalNode {
    fn new(uuid: Uuid, depth: usize, name: String, node_type: String) -> Self {
        Self {
            uuid,
            depth,
            name,
            node_type,
            relations: vec![],
            relation_ids: vec![],
        }
    }

    pub fn from(db_entries: Vec<DbRelation>, depth: usize) -> Option<TraversalNode> {
        let n = db_entries.get(0)?;
        let mut node = TraversalNode::new(n.uuid, depth, n.name.clone(), n.node_type.clone());

        let mut ids = vec![];
        for i in 1..db_entries.len() {
            let r = db_entries.get(i)?;
            ids.push(r.relates_to.clone().unwrap());
        }

        node.relation_ids = ids;

        Some(node)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Relation {
    #[serde(rename = "type")]
    pub rel_type: String,
    pub outbound: bool,
    pub target_name: String,
    pub relates_to: String,
}

impl Relation {
    pub fn from(name: String, rel_type: String, relates_to: String, outbound: bool) -> Self {
        Self {
            rel_type,
            target_name: name,
            relates_to,
            outbound,
        }
    }
}
