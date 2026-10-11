use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

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

    if let Ok(control) = env::var("CROWDB_ICEBERG_RUST_RETIRE_CONTROL") {
        catalog.create_namespace(&namespace, HashMap::new()).await?;
        let table = TableIdent::new(namespace.clone(), "rust_retired".to_owned());
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
                    .schema(schema.clone())
                    .build(),
            )
            .await?;
        second_catalog.load_table(&table).await?;
        let mut control = tokio::net::TcpStream::connect(control).await?;
        control.write_all(&[1]).await?;
        control.read_exact(&mut [0]).await?;
        assert!(catalog.load_table(&table).await.is_err());
        assert!(!second_catalog.namespace_exists(&namespace).await?);
        second_catalog.create_namespace(&namespace, HashMap::new()).await?;
        second_catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name(table.name().to_owned())
                    .schema(schema)
                    .build(),
            )
            .await?;
        catalog.load_table(&table).await?;
        second_catalog.drop_table(&table).await?;
        catalog.drop_namespace(&namespace).await?;
        return Ok(());
    }

    if env::var_os("CROWDB_ICEBERG_RUST_VERIFY_EXISTING").is_some() {
        let table = TableIdent::new(namespace.clone(), "rust_lost_reply".to_owned());
        assert!(second_catalog.namespace_exists(&namespace).await?);
        assert!(catalog.table_exists(&table).await?);
        second_catalog.load_table(&table).await?;
        catalog.drop_table(&table).await?;
        second_catalog.drop_namespace(&namespace).await?;
        return Ok(());
    }

    if env::var_os("CROWDB_ICEBERG_RUST_RESPONSE_LOSS").is_some() {
        second_catalog.create_namespace(&namespace, HashMap::new()).await?;
        let table = TableIdent::new(namespace.clone(), "rust_lost_reply".to_owned());
        let schema = Schema::builder()
            .with_fields(vec![NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            )
            .into()])
            .build()?;
        assert!(catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name(table.name().to_owned())
                    .schema(schema)
                    .build(),
            )
            .await
            .is_err());
        assert!(second_catalog.table_exists(&table).await?);
        second_catalog.load_table(&table).await?;
        if env::var_os("CROWDB_ICEBERG_RUST_KEEP_TABLE").is_none() {
            second_catalog.drop_table(&table).await?;
            second_catalog.drop_namespace(&namespace).await?;
        }
        return Ok(());
    }

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
    let renamed = TableIdent::new(namespace.clone(), "rust_renamed".to_owned());
    second_catalog.rename_table(&table, &renamed).await?;
    assert!(!second_catalog.table_exists(&table).await?);
    assert!(catalog.table_exists(&renamed).await?);
    catalog.load_table(&renamed).await?;
    catalog.drop_table(&renamed).await?;
    assert!(!second_catalog.table_exists(&renamed).await?);
    catalog.drop_namespace(&namespace).await?;
    assert!(!second_catalog.namespace_exists(&namespace).await?);
    Ok(())
}
