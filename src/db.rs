use sqlx::{PgPool, migrate, migrate::Migrator};

pub fn migrator() -> Migrator {
    let mut migrator = migrate!();
    migrator.create_schema("pgspawn");
    migrator.dangerous_set_table_name("pgspawn.schema_migrations");
    migrator
}

pub async fn migrate(db: &PgPool) -> Result<(), sqlx::Error> {
    migrator().run(db).await?;
    Ok(())
}
