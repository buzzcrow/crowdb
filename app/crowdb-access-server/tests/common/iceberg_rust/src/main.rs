use std::collections::HashMap;
use std::env;
use std::sync::Arc;

use iceberg::io::MemoryStorageFactory;
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_rest::RestCatalogBuilder;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let origin = env::var("CROWDB_ICEBERG_RUST_ORIGIN")?;
    let second_origin = env::var("CROWDB_ICEBERG_RUST_SECOND_ORIGIN")?;
    let token = env::var("CROWDB_ICEBERG_RUST_TOKEN")?;
    let namespace = NamespaceIdent::new(env::var("CROWDB_ICEBERG_RUST_NAMESPACE")?);
    let catalog = RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(MemoryStorageFactory))
        .load(
            "crowdb",
            HashMap::from([("uri".to_owned(), origin), ("token".to_owned(), token.clone())]),
        )
        .await?;
    let second_catalog = RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(MemoryStorageFactory))
        .load(
            "crowdb",
            HashMap::from([("uri".to_owned(), second_origin), ("token".to_owned(), token)]),
        )
        .await?;

    assert!(!catalog.namespace_exists(&namespace).await?);
    catalog.create_namespace(&namespace, HashMap::new()).await?;
    assert!(catalog.namespace_exists(&namespace).await?);
    assert!(second_catalog.namespace_exists(&namespace).await?);
    assert!(catalog.list_namespaces(None).await?.contains(&namespace));
    assert_eq!(catalog.get_namespace(&namespace).await?.name(), &namespace);
    assert!(catalog.list_tables(&namespace).await?.is_empty());
    let table = TableIdent::new(namespace.clone(), "rust_table".to_owned());
    assert!(!catalog.table_exists(&table).await?);
    let schema = Schema::builder()
        .with_fields(vec![NestedField::required(
            1,
            "id",
            Type::Primitive(PrimitiveType::Long),
        )
        .into()])
        .build()?;
    catalog
        .create_table(
            &namespace,
            TableCreation::builder()
                .name(table.name().to_owned())
                .schema(schema)
                .build(),
        )
        .await?;
    assert!(catalog.table_exists(&table).await?);
    assert!(second_catalog.table_exists(&table).await?);
    assert!(second_catalog.list_tables(&namespace).await?.contains(&table));
    second_catalog.load_table(&table).await?;
    catalog.drop_table(&table).await?;
    assert!(!second_catalog.table_exists(&table).await?);
    catalog.drop_namespace(&namespace).await?;
    assert!(!second_catalog.namespace_exists(&namespace).await?);
    Ok(())
}
