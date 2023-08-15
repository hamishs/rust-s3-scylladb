mod api;
mod config;
mod data;
mod db;

extern crate num_cpus;
extern crate serde_json;

use crate::api::{create_node, get_by_id, traversal_by_id, AppState};
use crate::config::Config;
use crate::db::scylladb::ScyllaDbService;
use actix_web::middleware::Logger;
use actix_web::{web::Data, App, HttpServer};
use color_eyre::Result;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::info;

#[actix_web::main]
async fn main() -> Result<()> {
    let config = Config::from_env().expect("Server configuration");

    let port = config.port;
    let host = config.host.clone();
    let num_cpus = num_cpus::get();
    let parallel_files = config.parallel_files;
    let db_parallelism = config.db_parallelism;
    let region = config.region;

    info!(
        "Starting application. Num CPUs {}. Max Parallel Files {}. DB Parallelism {}.  Region {}",
        num_cpus, parallel_files, db_parallelism, region
    );

    let db = ScyllaDbService::new(
        config.db_dc,
        config.db_url,
        db_parallelism,
        config.schema_file,
    )
    .await;

    let sem = Arc::new(Semaphore::new(parallel_files));
    let data = Data::new(AppState {
        db_svc: db,
        semaphore: sem,
        region,
    });

    info!("Starting server at http://{}:{}/", host, port);
    HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .app_data(data.clone())
            .service(get_by_id)
            .service(traversal_by_id)
            .service(create_node)
    })
    .bind(format!("{}:{}", host, port))?
    .workers(num_cpus * 2)
    .run()
    .await?;

    Ok(())
}
