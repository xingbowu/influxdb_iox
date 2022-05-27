//! Interface logic between IOx ['Predicate`] and the predicates used
//! by the InfluxDB Storage gRPC API
mod field_rewrite;

use crate::{rewrite, Predicate, ValueExpr};

use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_plan::{
    lit, Column, Expr, ExprRewritable, ExprRewriter, ExprSchema, ExprSchemable, ExprSimplifiable,
    SimplifyInfo,
};
use schema::Schema;
use std::collections::BTreeSet;
use std::sync::Arc;

use self::field_rewrite::{FieldProjection, FieldProjectionRewriter};

/// Any column references to this name are rewritten to be
/// the actual table name by the Influx gRPC planner.
///
/// This is required to support predicates like
/// `_measurement = "foo" OR tag1 = "bar"`
///
/// The plan for each table will have the value of `_measurement`
/// filled in with a literal for the respective name of that field
pub const MEASUREMENT_COLUMN_NAME: &str = "_measurement";

/// A reference to a field's name which is used to represent column
/// projections in influx RPC predicates.
///
/// For example, a predicate like
/// ```text
/// _field = temperature
/// ```
///
/// Means to select only the (field) column named "temperature"
///
/// Any equality expressions using this column name are removed and
/// replaced with projections on the specified column.
pub const FIELD_COLUMN_NAME: &str = "_field";

/// Any column references to this name are rewritten to be a disjunctive set of
/// expressions to all field columns for the table schema.
///
/// This is required to support predicates like
/// `_value` = 1.77
///
/// The plan for each table will have expression containing `_value` rewritten
/// into multiple expressions (one for each field column).
pub const VALUE_COLUMN_NAME: &str = "_value";

/// Predicate used by the InfluxDB Storage gRPC API
#[derive(Debug, Clone, Default)]
pub struct InfluxRpcPredicate {
    /// Optional table restriction. If present, restricts the results
    /// to only tables whose names are in `table_names`
    table_names: Option<BTreeSet<String>>,

    /// The inner predicate
    inner: Predicate,
}

impl InfluxRpcPredicate {
    /// Create a new [`InfluxRpcPredicate`]
    pub fn new(table_names: Option<BTreeSet<String>>, predicate: Predicate) -> Self {
        Self {
            table_names,
            inner: predicate,
        }
    }

    /// Create a new [`InfluxRpcPredicate`] with the given table
    pub fn new_table(table: impl Into<String>, predicate: Predicate) -> Self {
        Self::new(Some(std::iter::once(table.into()).collect()), predicate)
    }

    /// Removes the timestamp range from this predicate, if the range
    /// is for the entire min/max valid range.
    ///
    /// This is used in certain cases to retain compatibility with the
    /// existing storage engine
    pub fn clear_timestamp_if_max_range(self) -> Self {
        Self {
            inner: self.inner.clear_timestamp_if_max_range(),
            ..self
        }
    }

    /// Convert to a list of [`Predicate`] to apply to specific tables
    ///
    /// Returns a list of [`Predicate`] and their associated table name
    pub fn table_predicates(
        &self,
        table_info: &dyn QueryDatabaseMeta,
    ) -> DataFusionResult<Vec<(String, Predicate)>> {
        let table_names = match &self.table_names {
            Some(table_names) => itertools::Either::Left(table_names.iter().cloned()),
            None => itertools::Either::Right(table_info.table_names().into_iter()),
        };

        table_names
            .map(|table| {
                let schema = table_info.table_schema(&table);
                let predicate = normalize_predicate(&table, schema, &self.inner)?;

                Ok((table, predicate))
            })
            .collect()
    }

    /// Returns the table names this predicate is restricted to if any
    pub fn table_names(&self) -> Option<&BTreeSet<String>> {
        self.table_names.as_ref()
    }

    /// Returns true if ths predicate evaluates to true for all rows
    pub fn is_empty(&self) -> bool {
        self.table_names.is_none() && self.inner.is_empty()
    }
}

/// Information required to normalize predicates
pub trait QueryDatabaseMeta {
    /// Returns a list of table names in this DB
    fn table_names(&self) -> Vec<String>;

    /// Schema for a specific table if the table exists.
    fn table_schema(&self, table_name: &str) -> Option<Arc<Schema>>;
}

/// Predicate that has been "specialized" / normalized for a
/// particular table. Specifically:
///
/// * all references to the [MEASUREMENT_COLUMN_NAME] column in any
/// `Exprs` are rewritten with the actual table name
/// * any expression on the [VALUE_COLUMN_NAME] column is rewritten to be
/// applied across all field columns.
/// * any expression on the [FIELD_COLUMN_NAME] is rewritten to be
/// applied for the particular fields.
///
/// For example if the original predicate was
/// ```text
/// _measurement = "some_table"
/// ```
///
/// When evaluated on table "cpu" then the predicate is rewritten to
/// ```text
/// "cpu" = "some_table"
/// ```
///
/// if the original predicate contained
/// ```text
/// _value > 34.2
/// ```
///
/// When evaluated on table "cpu" then the expression is rewritten as a
/// collection of disjunctive expressions against all field columns
/// ```text
/// ("field1" > 34.2 OR "field2" > 34.2 OR "fieldn" > 34.2)
/// ```
fn normalize_predicate(
    table_name: &str,
    schema: Option<Arc<Schema>>,
    predicate: &Predicate,
) -> DataFusionResult<Predicate> {
    let mut predicate = predicate.clone();
    let mut field_projections = FieldProjectionRewriter::new();
    let mut field_value_exprs = vec![];

    predicate.exprs = predicate
        .exprs
        .into_iter()
        .map(|e| {
            rewrite_measurement_references(table_name, e)
                // Rewrite any references to `_value = some_value` to literal true values.
                // Keeps track of these expressions, which can then be used to
                // augment field projections with conditions using `CASE` statements.
                .and_then(|e| rewrite_field_value_references(&mut field_value_exprs, e))
                // Rewrite any references to `_field = a_field_name` with a literal true
                // and keep track of referenced field names to add to the field
                // column projection set.
                .and_then(|e| field_projections.rewrite_field_exprs(e))
                // apply IOx specific rewrites (that unlock other simplifications)
                .and_then(rewrite::rewrite)
                // Call the core DataFusion simplification logic
                .and_then(|e| {
                    if let Some(schema) = &schema {
                        let adapter = SimplifyAdapter::new(schema.as_ref());
                        // simplify twice to ensure "full" cleanup
                        e.simplify(&adapter)?.simplify(&adapter)
                    } else {
                        Ok(e)
                    }
                })
                .and_then(rewrite::simplify_predicate)
        })
        .collect::<DataFusionResult<Vec<_>>>()?;

    // Store any field value (`_value`) expressions on the `Predicate`.
    predicate.value_expr = field_value_exprs;

    match field_projections.into_projection() {
        FieldProjection::None => {}
        FieldProjection::Include(include_field_names) => {
            predicate.add_field_names(include_field_names)
        }
        FieldProjection::Exclude(exclude_field_names) => {
            // if we don't have the schema, it means the table doesn't exist so we can safely ignore
            if let Some(schema) = schema {
                // add all fields other than the ones mentioned

                let new_fields = schema.fields_iter().filter_map(|f| {
                    let field_name = f.name();
                    if !exclude_field_names.contains(field_name) {
                        Some(field_name)
                    } else {
                        None
                    }
                });

                predicate.add_field_names(new_fields)
            }
        }
    }
    Ok(predicate)
}

struct SimplifyAdapter<'a> {
    schema: &'a Schema,
    execution_props: ExecutionProps,
}

impl<'a> SimplifyAdapter<'a> {
    fn new(schema: &'a Schema) -> Self {
        Self {
            schema,
            execution_props: ExecutionProps::new(),
        }
    }

    // returns the field named 'name', if any
    fn field(&self, name: &str) -> Option<&arrow::datatypes::Field> {
        self.schema
            .find_index_of(name)
            .map(|index| self.schema.field(index).1)
    }
}

impl<'a> SimplifyInfo for SimplifyAdapter<'a> {
    fn is_boolean_type(&self, expr: &Expr) -> DataFusionResult<bool> {
        Ok(expr
            .get_type(self)
            .ok()
            .map(|t| matches!(t, arrow::datatypes::DataType::Boolean))
            .unwrap_or(false))
    }

    fn nullable(&self, expr: &Expr) -> DataFusionResult<bool> {
        Ok(expr.nullable(self).ok().unwrap_or(false))
    }

    fn execution_props(&self) -> &ExecutionProps {
        &self.execution_props
    }
}

impl<'a> ExprSchema for SimplifyAdapter<'a> {
    fn nullable(&self, col: &Column) -> DataFusionResult<bool> {
        assert!(col.relation.is_none());
        //if the field isn't present IOx will treat it as null
        Ok(self
            .field(&col.name)
            .map(|f| f.is_nullable())
            .unwrap_or(true))
    }

    fn data_type(&self, col: &Column) -> DataFusionResult<&arrow::datatypes::DataType> {
        assert!(col.relation.is_none());
        self.field(&col.name)
            .map(|f| f.data_type())
            .ok_or_else(|| DataFusionError::Plan(format!("Unknown field {}", &col.name)))
    }
}

/// Rewrites all references to the [MEASUREMENT_COLUMN_NAME] column
/// with the actual table name
fn rewrite_measurement_references(table_name: &str, expr: Expr) -> DataFusionResult<Expr> {
    let mut rewriter = MeasurementRewriter { table_name };
    expr.rewrite(&mut rewriter)
}

struct MeasurementRewriter<'a> {
    table_name: &'a str,
}

impl ExprRewriter for MeasurementRewriter<'_> {
    fn mutate(&mut self, expr: Expr) -> DataFusionResult<Expr> {
        Ok(match expr {
            // rewrite col("_measurement") --> "table_name"
            Expr::Column(Column { relation, name }) if name == MEASUREMENT_COLUMN_NAME => {
                // should not have a qualified foo._measurement
                // reference
                assert!(relation.is_none());
                lit(self.table_name)
            }
            // no rewrite needed
            _ => expr,
        })
    }
}

/// Rewrites an expression on `_value` as a boolean true literal, pushing any
/// encountered expressions onto `value_exprs` so they can be moved onto column
/// projections.
fn rewrite_field_value_references(
    value_exprs: &mut Vec<ValueExpr>,
    expr: Expr,
) -> DataFusionResult<Expr> {
    let mut rewriter = FieldValueRewriter { value_exprs };
    expr.rewrite(&mut rewriter)
}

struct FieldValueRewriter<'a> {
    value_exprs: &'a mut Vec<ValueExpr>,
}

impl<'a> ExprRewriter for FieldValueRewriter<'a> {
    fn mutate(&mut self, expr: Expr) -> DataFusionResult<Expr> {
        // try and convert Expr into a ValueExpr
        match expr.try_into() {
            // found a value expr. Save and replace with true
            Ok(value_expr) => {
                self.value_exprs.push(value_expr);
                Ok(lit(true))
            }
            // not a ValueExpr, so leave the same
            Err(expr) => Ok(expr),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_plan::col;

    #[test]
    fn test_field_value_rewriter() {
        let mut rewriter = FieldValueRewriter {
            value_exprs: &mut vec![],
        };

        let cases = vec![
            (col("f1").eq(lit(1.82)), col("f1").eq(lit(1.82)), vec![]),
            (col("t2"), col("t2"), vec![]),
            (
                col(VALUE_COLUMN_NAME).eq(lit(1.82)),
                // _value = 1.82 -> true
                lit(true),
                vec![ValueExpr {
                    expr: col(VALUE_COLUMN_NAME).eq(lit(1.82)),
                }],
            ),
        ];

        for (input, exp, mut value_exprs) in cases {
            let rewritten = input.rewrite(&mut rewriter).unwrap();
            assert_eq!(rewritten, exp);
            assert_eq!(rewriter.value_exprs, &mut value_exprs);
        }

        // Test case with single field.
        let mut rewriter = FieldValueRewriter {
            value_exprs: &mut vec![],
        };

        let input = col(VALUE_COLUMN_NAME).gt(lit(1.88));
        let rewritten = input.clone().rewrite(&mut rewriter).unwrap();
        assert_eq!(rewritten, lit(true));
        assert_eq!(rewriter.value_exprs, &mut vec![ValueExpr { expr: input }]);
    }
}
