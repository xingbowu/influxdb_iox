pub(crate) mod influxrpc;
mod multi_ingester;

use std::time::Duration;

use assert_cmd::Command;
use futures::FutureExt;
use predicates::prelude::*;
use test_helpers::timeout::FutureTimeout;
use test_helpers_end_to_end::{
    maybe_skip_integration, run_query, try_run_query, MiniCluster, Step, StepTest, StepTestState,
    TestConfig,
};

#[tokio::test]
async fn basic_ingester() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    let table_name = "the_table";

    // Set up the cluster  ====================================
    let mut cluster = MiniCluster::create_shared(database_url).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(format!(
                "{},tag1=A,tag2=B val=42i 123456\n\
                 {},tag1=A,tag2=C val=43i 123457",
                table_name, table_name
            )),
            Step::WaitForReadable,
            Step::AssertNotPersisted,
            Step::Query {
                sql: format!("select * from {}", table_name),
                expected: vec![
                    "+------+------+--------------------------------+-----+",
                    "| tag1 | tag2 | time                           | val |",
                    "+------+------+--------------------------------+-----+",
                    "| A    | B    | 1970-01-01T00:00:00.000123456Z | 42  |",
                    "| A    | C    | 1970-01-01T00:00:00.000123457Z | 43  |",
                    "+------+------+--------------------------------+-----+",
                ],
            },
        ],
    )
    .run()
    .await
}

#[tokio::test]
async fn basic_on_parquet() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    let table_name = "the_table";

    // Set up the cluster  ====================================
    let mut cluster = MiniCluster::create_shared(database_url).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(format!("{},tag1=A,tag2=B val=42i 123456", table_name)),
            // Wait for data to be persisted to parquet
            Step::WaitForPersisted,
            Step::Query {
                sql: format!("select * from {}", table_name),
                expected: vec![
                    "+------+------+--------------------------------+-----+",
                    "| tag1 | tag2 | time                           | val |",
                    "+------+------+--------------------------------+-----+",
                    "| A    | B    | 1970-01-01T00:00:00.000123456Z | 42  |",
                    "+------+------+--------------------------------+-----+",
                ],
            },
        ],
    )
    .run()
    .await
}

#[tokio::test]
async fn basic_delete() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    let table_name = "the_table";

    // Set up the cluster  ====================================
    let mut cluster = MiniCluster::create_shared(database_url).await;

    let sql = format!("select * from {}", table_name);

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(format!("{},tag1=A,tag2=B val=42i 123456", table_name)),
            Step::WriteLineProtocol(format!("{},tagA=X,tag2=Y val=42i 123457", table_name)),
            Step::WaitForReadable,
            Step::Query {
                sql: sql.clone(),
                expected: vec![
                    "+------+------+------+--------------------------------+-----+",
                    "| tag1 | tag2 | tagA | time                           | val |",
                    "+------+------+------+--------------------------------+-----+",
                    "|      | Y    | X    | 1970-01-01T00:00:00.000123457Z | 42  |",
                    "| A    | B    |      | 1970-01-01T00:00:00.000123456Z | 42  |",
                    "+------+------+------+--------------------------------+-----+",
                ],
            },
            // now delete the data
            Step::Delete {
                predicate: format!(r#"_measurement = "{}" AND tag1="A""#, table_name),
                start: 0,
                stop: 1000000,
            },
            // Wait for delete to be readable
            //Step::WaitForReadable,
            // workaround the lack of write token for a delete
            // https://github.com/influxdata/influxdb_iox/issues/4209
            // TODO file a ticket to add deletes to write token
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                let cluster = state.cluster();

                // query in a loop until the delete is visible
                async move {
                    loop {
                        let batches = run_query(
                            &sql,
                            cluster.namespace(),
                            cluster.querier().querier_grpc_connection(),
                        )
                        .await;
                        let num_rows = batches.iter().map(|b| b.num_rows()).sum::<usize>();
                        if num_rows == 1 {
                            return;
                        } else {
                            println!("Got {} rows, waiting for 1", num_rows);
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                }
                .with_timeout_panic(Duration::from_secs(10))
                .boxed()
            })),
            // row should be gone now
            Step::Query {
                sql: format!("select * from {}", table_name),
                expected: vec![
                    "+------+------+------+--------------------------------+-----+",
                    "| tag1 | tag2 | tagA | time                           | val |",
                    "+------+------+------+--------------------------------+-----+",
                    "|      | Y    | X    | 1970-01-01T00:00:00.000123457Z | 42  |",
                    "| A    | B    |      | 1970-01-01T00:00:00.000123456Z | 42  |",
                    "+------+------+------+--------------------------------+-----+",
                ],
            },
        ],
    )
    .run()
    .await
}

#[tokio::test]
async fn basic_no_ingester_connection() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    let table_name = "the_table";

    let router_config = TestConfig::new_router(&database_url);
    // fast parquet
    let ingester_config = TestConfig::new_ingester(&router_config);

    // specially create a querier config that is NOT connected to the ingester
    let querier_config = TestConfig::new_querier_without_ingester(&ingester_config);

    // Set up the cluster  ====================================
    let mut cluster = MiniCluster::new()
        .with_router(router_config)
        .await
        .with_ingester(ingester_config)
        .await
        .with_querier(querier_config)
        .await;

    // Write some data into the v2 HTTP API ==============
    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(format!("{},tag1=A,tag2=B val=42i 123456", table_name)),
            // Wait for data to be persisted to parquet, ask the ingester directly because the
            // querier is not connected to the ingester
            Step::WaitForPersistedAccordingToIngester,
            Step::Query {
                sql: format!("select * from {}", table_name),
                expected: vec![
                    "+------+------+--------------------------------+-----+",
                    "| tag1 | tag2 | time                           | val |",
                    "+------+------+--------------------------------+-----+",
                    "| A    | B    | 1970-01-01T00:00:00.000123456Z | 42  |",
                    "+------+------+--------------------------------+-----+",
                ],
            },
        ],
    )
    .run()
    .await
}

#[tokio::test]
async fn table_not_found_on_ingester() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    let table_name = "the_table";

    // Set up the cluster  ====================================
    let mut cluster = MiniCluster::create_shared(database_url).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(format!("{},tag1=A,tag2=B val=42i 123456", table_name)),
            Step::WaitForPersisted,
            Step::WriteLineProtocol(String::from("other_table,tag1=A,tag2=B val=42i 123456")),
            Step::WaitForPersisted,
            // Restart the ingester so that it does not have any table data in memory
            // and so will return "not found" to the querier
            Step::Custom(Box::new(|state: &mut StepTestState| {
                state.cluster_mut().restart_ingester().boxed()
            })),
            Step::Query {
                sql: format!("select * from {}", table_name),
                expected: vec![
                    "+------+------+--------------------------------+-----+",
                    "| tag1 | tag2 | time                           | val |",
                    "+------+------+--------------------------------+-----+",
                    "| A    | B    | 1970-01-01T00:00:00.000123456Z | 42  |",
                    "+------+------+--------------------------------+-----+",
                ],
            },
        ],
    )
    .run()
    .await
}

#[tokio::test]
async fn ingester_panic() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    let table_name = "the_table";

    // Set up the cluster  ====================================
    let router_config = TestConfig::new_router(&database_url);
    // can't use standard mini cluster here as we setup the querier to panic
    let ingester_config =
        TestConfig::new_ingester(&router_config).with_ingester_flight_do_get_panic(2);
    let querier_config = TestConfig::new_querier(&ingester_config).with_json_logs();
    let mut cluster = MiniCluster::new()
        .with_router(router_config)
        .await
        .with_ingester(ingester_config)
        .await
        .with_querier(querier_config)
        .await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(format!(
                "{},tag1=A,tag2=B val=42i 123456\n\
                 {},tag1=A,tag2=C val=43i 123457",
                table_name, table_name
            )),
            Step::WaitForReadable,
            Step::AssertNotPersisted,
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                async move {
                    // SQL query fails, error is propagated
                    let sql = format!("select * from {} where tag2='B'", table_name);
                    let err = try_run_query(
                        sql,
                        state.cluster().namespace(),
                        state.cluster().querier().querier_grpc_connection(),
                    )
                    .await
                    .unwrap_err();
                    if let influxdb_iox_client::flight::Error::GrpcError(status) = err {
                        assert_eq!(status.code(), tonic::Code::Internal);
                    } else {
                        panic!("wrong error type");
                    }

                    // find relevant log line for debugging
                    let querier_logs =
                        std::fs::read_to_string(state.cluster().querier().log_path().await)
                            .unwrap();
                    let log_line = querier_logs
                        .split('\n')
                        .find(|s| s.contains("Failed to perform ingester query"))
                        .unwrap();
                    let log_data: serde_json::Value = serde_json::from_str(log_line).unwrap();
                    let log_data = log_data.as_object().unwrap();
                    let log_data = log_data["fields"].as_object().unwrap();

                    // query ingester using debug information
                    for i in 0..2 {
                        let assert = Command::cargo_bin("influxdb_iox")
                            .unwrap()
                            .arg("-h")
                            .arg(log_data["ingester_address"].as_str().unwrap())
                            .arg("query-ingester")
                            .arg(log_data["namespace"].as_str().unwrap())
                            .arg(log_data["table"].as_str().unwrap())
                            .arg("--columns")
                            .arg(log_data["columns"].as_str().unwrap())
                            .arg("--predicate-base64")
                            .arg(log_data["predicate_binary"].as_str().unwrap())
                            .assert();

                        // The ingester is configured to fail 2 times, once for the original query and once during
                        // debugging. The 2nd debug query should work and should only return data for `tag2=B` (not for
                        // `tag2=C`).
                        if i == 0 {
                            assert.failure().stderr(predicate::str::contains(
                                "Error querying: status: Internal, message: \"internal error, sad kittens\"",
                            ));
                        } else {
                            assert.success().stdout(
                                predicate::str::contains(
                                    "| A    | B    | 1970-01-01T00:00:00.000123456Z | 42  |",
                                )
                                .and(predicate::str::contains("C").not()),
                            );
                        }
                    }
                }
                .boxed()
            })),
        ],
    )
    .run()
    .await
}
