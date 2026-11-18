use std::rc::Rc;

use r#type::{Error, FromRow, GenericClient, Row};

struct Value(Rc<i32>);

impl FromRow<'_> for Value {
    fn from_row(row: &Row) -> Result<Self, Error> {
        let value = row.try_get(0)?;
        Ok(Self(Rc::new(value)))
    }
}

async fn query<C: GenericClient>(client: &C) -> Result<(), Error> {
    let _: Vec<Value> = client.query_as("SELECT 1", &[]).await?;
    let _: Value = client.query_one_as("SELECT 1", &[]).await?;
    let _: Option<Value> = client.query_opt_as("SELECT 1", &[]).await?;

    Ok(())
}

fn main() {}
