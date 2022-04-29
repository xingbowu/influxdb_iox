use async_stream::try_stream;
use either::Either;
use futures::future::BoxFuture;
use futures::prelude::stream::BoxStream;
use futures_util::{pin_mut, TryStreamExt};
use iox_time::{SystemProvider, TimeProvider};
use sqlx::database::HasStatement;
use sqlx::pool::PoolConnection;
use sqlx::{Acquire, Database, Describe, Error, Execute, Executor, Pool, Transaction};
use std::fmt::Debug;
use std::time::Duration;

pub trait AcquireDurationMeter: Debug + Send + Sync {
    fn record_acquire_duration(&self, d: Duration);
}

#[derive(Debug)]
pub struct AcquireDurationMeasuringPool<DB, M, P = SystemProvider>
where
    DB: Database,
    M: AcquireDurationMeter,
{
    pool: Pool<DB>,
    meter: M,
    time_provider: P,
}

impl<DB, M> AcquireDurationMeasuringPool<DB, M>
where
    DB: Database,
    M: AcquireDurationMeter,
{
    pub fn new(pool: Pool<DB>, meter: M) -> Self {
        Self {
            pool,
            meter,
            time_provider: Default::default(),
        }
    }

    pub fn time_provider<P: TimeProvider>(
        self,
        time_provider: P,
    ) -> AcquireDurationMeasuringPool<DB, M, P> {
        AcquireDurationMeasuringPool {
            pool: self.pool,
            meter: self.meter,
            time_provider,
        }
    }
}

impl<'a, DB, M, P> Acquire<'a> for &'a AcquireDurationMeasuringPool<DB, M, P>
where
    DB: Database,
    M: AcquireDurationMeter,
    P: TimeProvider,
{
    type Database = DB;

    type Connection = PoolConnection<DB>;

    fn acquire(self) -> BoxFuture<'a, Result<Self::Connection, Error>> {
        Box::pin(async move {
            let start = self.time_provider.now();
            let conn = self.pool.acquire().await?;
            // Avoid exploding if time goes backwards - simply drop the measurement
            // if it happens.
            if let Some(delta) = self.time_provider.now().checked_duration_since(start) {
                self.meter.record_acquire_duration(delta);
            }
            Ok(conn)
        })
    }

    fn begin(self) -> BoxFuture<'a, Result<Transaction<'a, DB>, Error>> {
        // We can't intercept "acquire" independently of the actual begin transaction overhead.
        // That's not a big deal since pool.begin() is just a practical helper around `acquire()` + `begin()`.
        //
        // The sqlx internals make it impossible. It's due to the way transactions can be created:
        // You can create a transaction either from a pool or from a connection.
        // If you create the transaction from a connection (by calling `conn.begin()`),
        // the transaction borrows the connection and thus the connection needs outlive the transaction.
        // This means that when the transaction is dropped there is no way to "drop" the connection.
        //
        // Pool connections OTOH rely on the ability to intercept the connection drop in order to put the
        // connection back into the pool. For that to work, `Transaction` needs to also implement `Drop` and
        // drop the pool connection when dropped (i.e. it needs to "own" the connection).
        //
        // `Transaction` handles those two cases (owning and reference) by using an internal "MaybePoolConnection"
        // enum which we can't use outside sqlx.
        unimplemented!("Use `pool.acquire()` to get a connection and then call `begin()` on it")
    }
}

impl<'p, DB, M> Executor<'p> for &'p AcquireDurationMeasuringPool<DB, M>
where
    DB: Database,
    M: AcquireDurationMeter,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
{
    type Database = DB;

    fn fetch_many<'e, 'q: 'e, E: 'q>(
        self,
        query: E,
    ) -> BoxStream<'e, Result<Either<DB::QueryResult, DB::Row>, Error>>
    where
        E: Execute<'q, Self::Database>,
        'p: 'e,
    {
        Box::pin(try_stream! {
          let mut conn = self.pool.acquire().await?;
          let s = conn.fetch_many(query);
          pin_mut!(s);
          while let Some(v) = s.try_next().await? {
            yield v;
          }
        })
    }

    fn fetch_optional<'e, 'q: 'e, E: 'q>(
        self,
        query: E,
    ) -> BoxFuture<'e, Result<Option<DB::Row>, Error>>
    where
        E: Execute<'q, Self::Database>,
        'p: 'e,
    {
        Box::pin(async move { self.pool.acquire().await?.fetch_optional(query).await })
    }

    fn prepare_with<'e, 'q: 'e>(
        self,
        sql: &'q str,
        parameters: &'e [<Self::Database as Database>::TypeInfo],
    ) -> BoxFuture<'e, Result<<Self::Database as HasStatement<'q>>::Statement, Error>>
    where
        'p: 'e,
    {
        Box::pin(async move {
            self.pool
                .acquire()
                .await?
                .prepare_with(sql, parameters)
                .await
        })
    }

    #[doc(hidden)]
    fn describe<'e, 'q: 'e>(
        self,
        sql: &'q str,
    ) -> BoxFuture<'e, Result<Describe<Self::Database>, Error>>
    where
        'p: 'e,
    {
        Box::pin(async move { self.pool.acquire().await?.describe(sql).await })
    }
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::ops::DerefMut;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use super::*;
    use iox_time::{MockProvider, Time};
    use rand::{distributions::Alphanumeric, Rng};
    use sqlx::{postgres::PgPoolOptions, Postgres};

    // Helper macro to skip tests if TEST_INTEGRATION and TEST_INFLUXDB_IOX_CATALOG_DSN environment variables
    // are not set.
    macro_rules! maybe_skip_integration {
        () => {{
            dotenv::dotenv().ok();

            let required_vars = ["TEST_INFLUXDB_IOX_CATALOG_DSN"];
            let unset_vars: Vec<_> = required_vars
                .iter()
                .filter_map(|&name| match env::var(name) {
                    Ok(_) => None,
                    Err(_) => Some(name),
                })
                .collect();
            let unset_var_names = unset_vars.join(", ");

            let force = env::var("TEST_INTEGRATION");

            if force.is_ok() && !unset_var_names.is_empty() {
                panic!(
                    "TEST_INTEGRATION is set, \
                            but variable(s) {} need to be set",
                    unset_var_names
                );
            } else if force.is_err() {
                eprintln!(
                    "skipping Postgres integration test - set {}TEST_INTEGRATION to run",
                    if unset_var_names.is_empty() {
                        String::new()
                    } else {
                        format!("{} and ", unset_var_names)
                    }
                );
                return;
            }
        }};
    }

    // test helper to create a regular DB connection
    async fn connect_db(max_connections: u32) -> Result<Pool<Postgres>, sqlx::Error> {
        // create a random schema for this particular pool
        let schema_name = {
            // use scope to make it clear to clippy / rust that `rng` is
            // not carried past await points
            let mut rng = rand::thread_rng();
            (&mut rng)
                .sample_iter(Alphanumeric)
                .filter(|c| c.is_ascii_alphabetic())
                .take(20)
                .map(char::from)
                .collect::<String>()
        };
        let dsn = std::env::var("TEST_INFLUXDB_IOX_CATALOG_DSN").unwrap();
        let captured_schema_name = schema_name.clone();
        PgPoolOptions::new()
            .min_connections(1)
            .max_connections(max_connections)
            .connect_timeout(Duration::from_secs(10))
            .idle_timeout(Duration::from_secs(500))
            .test_before_acquire(true)
            .after_connect(move |c| {
                let captured_schema_name = captured_schema_name.clone();
                Box::pin(async move {
                    // Tag the connection with the provided application name.
                    c.execute(sqlx::query("SET application_name = 'test';"))
                        .await?;

                    // Note can only bind data values, not schema names
                    let query = format!("CREATE SCHEMA IF NOT EXISTS {}", &captured_schema_name);
                    c.execute(sqlx::query(&query))
                        .await
                        .expect("failed to create schema");

                    let search_path_query = format!("SET search_path TO {}", captured_schema_name);
                    c.execute(sqlx::query(&search_path_query)).await?;
                    Ok(())
                })
            })
            .connect(&dsn)
            .await
    }

    #[derive(Debug)]
    struct CollectedMeter {
        durations: Arc<Mutex<Vec<Duration>>>,
    }

    impl AcquireDurationMeter for CollectedMeter {
        fn record_acquire_duration(&self, duration: Duration) {
            let mut v = self.durations.lock().expect("not poisoned");
            v.push(duration);
        }
    }

    #[tokio::test]
    async fn test_acquire() {
        // If running an integration test on your laptop, this requires that you have Postgres
        // running and that you've done the sqlx migrations. See the README in this crate for
        // info to set it up.
        maybe_skip_integration!();
        println!("tests are running");

        let durations = Arc::new(Mutex::new(vec![]));
        let collected_meter = CollectedMeter {
            durations: Arc::clone(&durations),
        };

        let pool = connect_db(1).await.unwrap();
        let time_provider = Arc::new(MockProvider::new(Time::MIN));
        let pool: AcquireDurationMeasuringPool<_, _, _> =
            AcquireDurationMeasuringPool::new(pool, collected_meter)
                .time_provider(Arc::clone(&time_provider));
        let pool = Arc::new(pool);

        let test_query_duration = Duration::from_secs(1);

        let tasks: Vec<_> = (0..4)
            .into_iter()
            .map(|_| {
                let pool = Arc::clone(&pool);
                let time_provider = Arc::clone(&time_provider);
                tokio::spawn(async move {
                    let mut conn = pool.acquire().await.expect("acquire");
                    let mut txn = conn.begin().await.expect("begin");
                    sqlx::query(r#"select 1"#)
                        .execute(txn.deref_mut())
                        .await
                        .expect("query");

                    time_provider.inc(test_query_duration);

                    txn.commit().await.expect("commit");
                })
            })
            .collect();
        futures::future::join_all(tasks).await;

        let mut durations = durations.lock().expect("not poisoned");
        durations.sort();
        for i in 0..3 {
            let delta = durations[i + 1] - durations[i];
            assert!(delta >= test_query_duration);
        }
    }
}
