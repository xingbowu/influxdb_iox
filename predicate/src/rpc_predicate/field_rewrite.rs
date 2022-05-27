//! Logic for rewriting _field = "val" expressions to projections

use super::FIELD_COLUMN_NAME;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_plan::{
    lit, Expr, ExprRewritable, ExprRewriter, ExprVisitable, ExpressionVisitor, Operator, Recursion,
};
use datafusion::scalar::ScalarValue;
use std::collections::BTreeSet;

/// What fields / columns should appear in the result
#[derive(Debug, PartialEq)]
pub(crate) enum FieldProjection {
    /// No restriction on fields/columns
    None,
    /// Only fields / columns that appear should be produced
    Include(BTreeSet<String>),
    /// Only fields / columns that are *NOT* in this list should appear
    Exclude(BTreeSet<String>),
}

impl FieldProjection {
    // Add `name` as a included field name
    fn try_add_include(&mut self, name: impl Into<String>) -> DataFusionResult<()> {
        match self {
            Self::None => {
                *self = Self::Include([name.into()].into_iter().collect());
            }
            Self::Include(cols) => {
                cols.insert(name.into());
            }
            Self::Exclude(_) => {
                return Err(DataFusionError::Plan(
                    "Unsupported _field predicate: previously had '!=' and now have '='".into(),
                ));
            }
        };
        Ok(())
    }

    // Add `name` as a excluded field name
    fn try_add_exclude(&mut self, name: impl Into<String>) -> DataFusionResult<()> {
        match self {
            Self::None => {
                *self = Self::Exclude([name.into()].into_iter().collect());
            }
            Self::Include(_) => {
                return Err(DataFusionError::Plan(
                    "Unsupported _field predicate: previously had '=' and now have '!='".into(),
                ));
            }
            Self::Exclude(cols) => {
                cols.insert(name.into());
            }
        };
        Ok(())
    }
}

/// Rewrites a predicate on `_field` as a projection on a specific defined by
/// the literal in the expression.
///
/// For example, the expression `_field = "load4"` is removed from the
/// normalised expression, and a column "load4" added to the predicate's
/// projection.
#[derive(Debug)]
pub(crate) struct FieldProjectionRewriter {
    /// Remember what we have seen for expressions across calls to
    /// ensure that multiple exprs don't end up having incompatible
    checker: FieldRefChecker,
    projection: FieldProjection,
}

impl FieldProjectionRewriter {
    pub(crate) fn new() -> Self {
        Self {
            checker: FieldRefChecker::new(),
            projection: FieldProjection::None,
        }
    }

    // Rewrites the predicate, looking for _field predicates
    pub(crate) fn rewrite_field_exprs(&mut self, expr: Expr) -> DataFusionResult<Expr> {
        let expr = expr
            .rewrite(&mut self.checker)?
            .rewrite(&mut FieldProjectionRewriterWrapper(self))?;
        expr.accept(FieldRefFinder {})?;
        Ok(expr)
    }

    /// Returns any projection found in the predicate
    pub(crate) fn into_projection(self) -> FieldProjection {
        self.projection
    }
}

// newtype so users aren't confused and use `FieldProjectionRewriter` directly
struct FieldProjectionRewriterWrapper<'a>(&'a mut FieldProjectionRewriter);

impl<'a> ExprRewriter for FieldProjectionRewriterWrapper<'a> {
    /// This looks for very specific patterns
    /// (_field = "foo") OR (_field = "bar") OR ...
    /// (_field != "foo") AND (_field != "bar") AND ...
    fn mutate(&mut self, expr: Expr) -> DataFusionResult<Expr> {
        match check_binary(&expr) {
            // col eq lit
            (Some(FIELD_COLUMN_NAME), Some(Operator::Eq), Some(name)) => {
                self.0.projection.try_add_include(name)?;
                return Ok(lit(true));
            }
            (Some(FIELD_COLUMN_NAME), Some(Operator::NotEq), Some(name)) => {
                self.0.projection.try_add_exclude(name)?;
                return Ok(lit(true));
            }
            _ => {}
        };

        Ok(expr)
    }
}

/// Returns (Some(op)) if this is a BinaryExpr where one of the inputs is `col = <string lit>`
fn check_binary_of_binary(expr: &Expr) -> Option<Operator> {
    if let Expr::BinaryExpr { left, op, right } = expr {
        if let (Some(_), Some(_), Some(_)) = check_binary(&left) {
            Some(*op)
        } else if let (Some(_), Some(_), Some(_)) = check_binary(&right) {
            Some(*op)
        } else {
            None
        }
    } else {
        None
    }
}

/// Returns (Some(field_name), op, Some(str)) if this expr represents `col = <string lit>`
fn check_binary(expr: &Expr) -> (Option<&str>, Option<Operator>, Option<&str>) {
    if let Expr::BinaryExpr { left, op, right } = expr {
        (as_col_name(&left), Some(*op), as_str_lit(&right))
    } else {
        (None, None, None)
    }
}

/// if `expr` is a column name, returns the name
fn as_col_name(expr: &Expr) -> Option<&str> {
    if let Expr::Column(inner) = expr {
        Some(inner.name.as_ref())
    } else {
        None
    }
}

/// if the expr is a non null literal string, returns the string value
fn as_str_lit(expr: &Expr) -> Option<&str> {
    if let Expr::Literal(ScalarValue::Utf8(Some(name))) = &expr {
        Some(name.as_str())
    } else {
        None
    }
}

/// Searches for `_field` and errors if found
struct FieldRefFinder {}

impl ExpressionVisitor for FieldRefFinder {
    fn pre_visit(self, expr: &Expr) -> DataFusionResult<Recursion<Self>> {
        match as_col_name(expr) {
            Some(FIELD_COLUMN_NAME) => Err(DataFusionError::Plan(
                "Unsupported _field predicate, could not rewrite for IOx".into(),
            )),
            _ => Ok(Recursion::Continue(self)),
        }
    }
}

/// searches for unsupported patterns of predicates that we can't
/// translate into an include or exclude list
#[derive(Debug)]
struct FieldRefChecker {
    /// has seen a _field = <lit>
    seen_eq: bool,
    /// has seen a _field != <lit>
    seen_not_eq: bool,
    /// has the pattern  `(.. AND <_field pred>)` or `(<_field pred> AND ...) been seen?
    seen_and_field_pred: bool,
    /// has the pattern  `(.. OR <_field pred>)` or `(<_field pred> OR ...) been seen?
    seen_or_field_pred: bool,
}

impl FieldRefChecker {
    fn new() -> Self {
        Self {
            seen_eq: false,
            seen_not_eq: false,
            seen_and_field_pred: false,
            seen_or_field_pred: false,
        }
    }

    /// Note that we saw (<expr> AND <expr>) where one of the exprs
    /// was _field reference, returning error if there was an
    /// incompatible plan
    fn saw_and(&mut self) -> DataFusionResult<()> {
        println!("  saw_and self={:?}", self);
        self.seen_and_field_pred = true;
        // if we have previously seen an OR predicate or a = predicate not compatible
        if self.seen_or_field_pred {
            Err(DataFusionError::Plan(
                "Unsupported _field predicate: saw AND after previously seeing OR".into(),
            ))
        } else if self.seen_eq {
            Err(DataFusionError::Plan(
                "Unsupported _field predicate: saw AND after previously seeing = _field predicate"
                    .into(),
            ))
        } else {
            Ok(())
        }
    }

    /// Note that we saw (<expr> AND <expr>) where one of the exprs
    /// was _field reference, returning error if there was an
    /// incompatible plan
    fn saw_or(&mut self) -> DataFusionResult<()> {
        println!("  saw_or self={:?}", self);
        self.seen_or_field_pred = true;
        // if we have previously seen an AND predicate or a != predicate not compatible
        if self.seen_and_field_pred {
            Err(DataFusionError::Plan(
                "Unsupported _field predicate: saw OR after previously seeing AND".into(),
            ))
        } else if self.seen_not_eq {
            Err(DataFusionError::Plan(
                "Unsupported _field predicate: saw OR after previously seeing != _field predicate"
                    .into(),
            ))
        } else {
            Ok(())
        }
    }
}

impl ExprRewriter for FieldRefChecker {
    fn mutate(&mut self, expr: Expr) -> DataFusionResult<Expr> {
        // Note field references (error from having both
        // is checked during the actual rewrite)
        match check_binary(&expr) {
            // col eq lit
            (Some(FIELD_COLUMN_NAME), Some(Operator::Eq), Some(_name)) => {
                self.seen_eq = true;
            }
            // col neq li
            (Some(FIELD_COLUMN_NAME), Some(Operator::NotEq), Some(_name)) => {
                self.seen_not_eq = true;
            }
            _ => {}
        };

        // check for incompatible AND/OR
        match check_binary_of_binary(&expr) {
            Some(Operator::And) => {
                self.saw_and()?;
            }
            Some(Operator::Or) => {
                self.saw_or()?;
            }
            _ => {}
        };

        Ok(expr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_plan::{case, col};
    use test_helpers::assert_contains;

    #[test]
    fn test_field_column_rewriter() {
        let cases = vec![
            (
                // f1 = 1.82
                col("f1").eq(lit(1.82)),
                col("f1").eq(lit(1.82)),
                FieldProjection::None,
            ),
            (
                // _field != "f1"
                field_ref().not_eq(lit("f1")),
                lit(true),
                exclude(vec!["f1"]),
            ),
            (
                // _field = f1
                field_ref().eq(lit("f1")),
                lit(true),
                include(vec!["f1"]),
            ),
            (
                // _field = f1 AND _measurement = m1
                field_ref()
                    .eq(lit("f1"))
                    .and(col("_measurement").eq(lit("m1"))),
                lit(true).and(col("_measurement").eq(lit("m1"))),
                include(vec!["f1"]),
            ),
            (
                // (_field = f1) OR (_field = f2)
                field_ref().eq(lit("f1")).or(field_ref().eq(lit("f2"))),
                lit(true).or(lit(true)),
                include(vec!["f1", "f2"]),
            ),
            (
                // (f1 = 5) AND (((_field = f1) OR (_field = f3)) OR (_field = f2))
                col("f1").eq(lit(5.0)).and(
                    field_ref()
                        .eq(lit("f1"))
                        .or(field_ref().eq(lit("f3")))
                        .or(field_ref().eq(lit("f2"))),
                ),
                col("f1")
                    .eq(lit(5.0))
                    .and(lit(true).or(lit(true)).or(lit(true))),
                include(vec!["f1", "f2", "f3"]),
            ),
            (
                // (_field != f1) AND (_field != f2)
                field_ref()
                    .not_eq(lit("f1"))
                    .and(field_ref().not_eq(lit("f2"))),
                lit(true).and(lit(true)),
                exclude(vec!["f1", "f2"]),
            ),
            (
                // (f1 = 5) AND (((_field != f1) AND (_field != f3)) AND (_field != f2))
                col("f1").eq(lit(5.0)).and(
                    field_ref()
                        .not_eq(lit("f1"))
                        .and(field_ref().not_eq(lit("f3")))
                        .and(field_ref().not_eq(lit("f2"))),
                ),
                col("f1")
                    .eq(lit(5.0))
                    .and(lit(true).and(lit(true)).and(lit(true))),
                exclude(vec!["f1", "f2", "f3"]),
            ),
        ];

        for (input, exp_expr, exp_projection) in cases {
            println!(
                "Running test\ninput: {:?}\nexpected_expr: {:?}\nexpected_projection: {:?}\n",
                input, exp_expr, exp_projection
            );
            let mut rewriter = FieldProjectionRewriter::new();

            let rewritten = rewriter.rewrite_field_exprs(input).unwrap();
            assert_eq!(rewritten, exp_expr);

            let field_projection = rewriter.into_projection();
            assert_eq!(field_projection, exp_projection);
        }
    }

    #[test]
    fn test_field_column_rewriter_unsupported() {
        let cases = vec![
            (
                // reverse operand order not supported
                // f1 = _field
                lit("f1").eq(field_ref()),
                "Unsupported _field predicate, could not rewrite for IOx",
            ),
            (
                // reverse operand order not supported
                // f1 != _field
                lit("f1").not_eq(field_ref()),
                "Unsupported _field predicate, could not rewrite for IOx",
            ),
            (
                // mismatched != and =
                // (_field != f1) AND (_field = f3)
                field_ref().not_eq(lit("f1")).and(field_ref().eq(lit("f3"))),
                "Error during planning: Unsupported _field predicate: saw AND after previously seeing = _field predicate",
            ),
            (
                // mismatched = and !=
                // (_field = f1) OR (_field != f3)
                field_ref().eq(lit("f1")).or(field_ref().not_eq(lit("f3"))),
                "Error during planning: Unsupported _field predicate: saw OR after previously seeing != _field predicate",
            ),
            (
                // mismatched AND and OR (unsupported)
                // (_field = f1) OR (_field = f3) AND (_field = f2)
                field_ref().eq(lit("f1")).or(field_ref().eq(lit("f3"))).and(field_ref().eq(lit("f2"))),
                "Error during planning: Unsupported _field predicate: saw AND after previously seeing OR",
            ),
            (
                // mismatched AND and OR (unsupported)
                // (_field = f1) AND (_field = f3) OR (_field = f2)
                field_ref().eq(lit("f1")).and(field_ref().eq(lit("f3"))).or(field_ref().eq(lit("f2"))),
                "Error during planning: Unsupported _field predicate: saw AND after previously seeing = _field predicate",
            ),
            (
                // mismatched OR
                // (_field != f1) OR (_field != f3)
                field_ref().not_eq(lit("f1")).or(field_ref().not_eq(lit("f3"))),
                "Error during planning: Unsupported _field predicate: saw OR after previously seeing != _field predicate",
            ),
            (
                // mismatched AND
                // (_field != f1) AND (_field = f3)
                field_ref().not_eq(lit("f1")).and(field_ref().eq(lit("f3"))),
                "Unsupported _field predicate: saw AND after previously seeing = _field predicate",
            ),
            (
                // bogus expr that we can't rewrite
                // _field IS NOT NULL
                field_ref().is_not_null(),
                "Error during planning: Unsupported _field predicate, could not rewrite for IOx",
            ),
            (
                // bogus expr that has a mix of = and !=
                // case X
                //   WHEN _field = "foo" THEN true
                //   WHEN  _field != "bar" THEN false
                // END
                case(col("x"))
                    .when(field_ref().eq(lit("foo")), lit(true))
                    .when(field_ref().not_eq(lit("bar")), lit(false))
                    .end()
                    .unwrap(),
                "Error during planning: Unsupported _field predicate: previously had '=' and now have '!='",
            ),
            (
                // bogus expr that has a mix of != and ==
                // case X
                //   WHEN  _field != "bar" THEN false
                //   WHEN _field = "foo" THEN true
                // END
                case(col("x"))
                    .when(field_ref().not_eq(lit("bar")), lit(false))
                    .when(field_ref().eq(lit("foo")), lit(true))
                    .end()
                    .unwrap(),
                "Error during planning: Unsupported _field predicate: previously had '!=' and now have '='",
            ),
        ];

        for (input, exp_error) in cases {
            println!(
                "Running test\ninput: {:?}\nexpected_error: {:?}\n",
                input, exp_error
            );
            let mut rewriter = FieldProjectionRewriter::new();
            let err = rewriter
                .rewrite_field_exprs(input)
                .expect_err("Expected error rewriting, but was successful");
            assert_contains!(err.to_string(), exp_error);
        }
    }

    /// returls a reference to the speciial _field column
    fn field_ref() -> Expr {
        col(FIELD_COLUMN_NAME)
    }

    /// Create a `FieldProjection::Include`
    fn include(names: impl IntoIterator<Item = &'static str>) -> FieldProjection {
        FieldProjection::Include(names.into_iter().map(|s| s.to_string()).collect())
    }

    /// Create a `FieldProjection::Include`
    fn exclude(names: impl IntoIterator<Item = &'static str>) -> FieldProjection {
        FieldProjection::Exclude(names.into_iter().map(|s| s.to_string()).collect())
    }
}
