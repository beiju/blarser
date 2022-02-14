use diesel::{Connection, insert_into, Insertable, QueryDsl, RunQueryDsl};
use diesel_derive_enum::DbEnum;
use uuid::Uuid;
use chrono::{DateTime, Utc};
use itertools::Itertools;
use diesel::prelude::*;
use rocket::info;

use crate::api::ChroniclerItem;
use crate::db::BlarserDbConn;
use crate::sim;
use crate::schema::*;
use crate::state::events_db::add_start_event;

#[derive(Insertable)]
#[table_name = "versions"]
struct NewVersion {
    ingest_id: i32,
    entity_type: &'static str,
    entity_id: Uuid,
    data: serde_json::Value,
    from_event: i32,
    next_timed_event: Option<DateTime<Utc>>,
}

#[derive(Insertable)]
#[table_name = "versions_parents"]
struct NewParent {
    parent: i32,
    child: i32,
}

impl NewVersion {
    fn for_initial_state(ingest_id: i32, from_event: i32, start_time: DateTime<Utc>, entity_type: &str, item: ChroniclerItem) -> Option<NewVersion> {
        let version = match entity_type {
            "sim" => Self::for_initial_state_typed::<sim::Sim>(ingest_id, from_event, start_time, item),
            "game" => Self::for_initial_state_typed::<sim::Game>(ingest_id, from_event, start_time, item),
            "team" => Self::for_initial_state_typed::<sim::Team>(ingest_id, from_event, start_time, item),
            "player" => Self::for_initial_state_typed::<sim::Player>(ingest_id, from_event, start_time, item),
            _ => {
                // TODO Remove this once all entity types are implemented
                return None;
            }
        };

        Some(version)
    }

    fn for_initial_state_typed<EntityT: sim::Entity>(ingest_id: i32, from_event: i32, start_time: DateTime<Utc>, item: ChroniclerItem) -> NewVersion {
        let raw: EntityT::Raw = serde_json::from_value(item.data)
            .expect("Couldn't deserialize entity into raw PartialInformation");

        let entity = EntityT::from_raw(raw);
        let next_timed_event = entity.next_timed_event(start_time)
            .map(|event| event.time);

        NewVersion {
            ingest_id,
            entity_type: EntityT::name(),
            entity_id: item.entity_id,
            data: serde_json::to_value(entity)
                .expect("Failed to serialize PartialInformation entity"),
            from_event,
            next_timed_event,
        }
    }
}

pub async fn add_initial_versions(conn: &BlarserDbConn, ingest_id: i32, start_time: DateTime<Utc>,
                                  versions: Vec<(&'static str, ChroniclerItem)>) {
    conn.run(move |c| {
        c.transaction(|| {
            let from_event = add_start_event(c, ingest_id, start_time);

            let chunks = versions.into_iter()
                .flat_map(move |(entity_type, item)| {
                    NewVersion::for_initial_state(ingest_id, from_event, start_time, entity_type, item)
                })
                .chunks(2000); // Diesel can't handle inserting the whole thing in one go

            let mut inserted = 0;
            for chunk in &chunks {
                use crate::schema::versions::dsl::*;
                inserted += insert_into(versions)
                    .values(chunk.collect::<Vec<_>>())
                    .execute(c)?;
                info!("Inserted {} initial versions", inserted);
            }

            Ok::<_, diesel::result::Error>(())
        })
    }).await
        .expect("Failed to save initial versions")
}

pub fn get_version_with_next_timed_event(c: &mut PgConnection, ingest_id: i32, before: DateTime<Utc>) -> Option<(String, serde_json::Value, DateTime<Utc>)> {
    use crate::schema::versions::dsl as versions;
    use crate::schema::versions_parents::dsl as parents;
    use crate::schema::events::dsl as events;
    versions::versions
        .inner_join(events::events.on(events::id.eq(versions::from_event)))
        .left_join(parents::versions_parents.on(parents::parent.eq(versions::id)))
        .select((versions::entity_type, versions::data, events::event_time))
        // From the proper ingest
        .filter(versions::ingest_id.eq(ingest_id))
        // Has a timed event before the requested time
        .filter(versions::next_timed_event.le(before))
        // Is a leaf node
        .filter(parents::child.is_null())
        // Is not terminated
        .filter(versions::terminated.is_null())
        // Get earliest
        .order(versions::next_timed_event.asc())
        .limit(1)
        .get_result::<(String, serde_json::Value, DateTime<Utc>)>(c)
        .optional()
        .expect("Error getting next version with timed event")
}

pub fn get_possible_versions_at(c: &PgConnection, ingest_id: i32, entity_type: &str, entity_id: Option<Uuid>, at_time: DateTime<Utc>) -> Vec<(i32, serde_json::Value)> {
    use crate::schema::versions::dsl as versions;
    use crate::schema::versions_parents::dsl as parents;
    use crate::schema::events::dsl as events;
    let base_query = versions::versions
        .select((versions::id, versions::data))
        .left_join(parents::versions_parents.on(parents::parent.eq(versions::id)))
        .inner_join(events::events.on(events::id.eq(versions::from_event)))
        // Is from the right ingest
        .filter(versions::ingest_id.eq(ingest_id))
        // Has the right entity type
        .filter(versions::entity_type.eq(entity_type))
        // Was created before the requested time
        .filter(events::event_time.le(at_time))
        // Has no children
        // TODO: Revisit this when it comes time to re-apply stored events to a past version after
        //   getting a new observation for it. This may not work, depending on whether I decide to
        //   delete the existing branch of the tree before generating a new one
        .filter(parents::child.is_null());

    match entity_id {
        Some(entity_id) => {
            base_query
                // Has the right entity id
                .filter(versions::entity_id.eq(entity_id))
                .get_results::<(i32, serde_json::Value)>(c)
        }
        None => {
            base_query.get_results::<(i32, serde_json::Value)>(c)
        }
    }.expect("Error getting next version with timed event")
}

pub fn save_successors<EntityT: sim::Entity>(c: &PgConnection, ingest_id: i32, from_event: i32, start_time: DateTime<Utc>, successors: Vec<(EntityT, Vec<i32>)>) {
    let (new_versions, parents): (Vec<_>, Vec<_>) = successors.into_iter().map(|(entity, parents)| {
        let next_timed_event = entity.next_timed_event(start_time)
            .map(|event| event.time);
        let version = NewVersion {
            ingest_id,
            entity_type: EntityT::name(),
            entity_id: entity.id(),
            data: serde_json::to_value(entity)
                .expect("Failed to serialize new version"),
            from_event,
            next_timed_event,
        };

        (version, parents)
    }).unzip();

    c.transaction(|| {
        use crate::schema::versions::dsl as versions;
        use crate::schema::versions_parents::dsl as parents;

        let children = insert_into(versions::versions)
            .values(new_versions)
            .returning(versions::id)
            .get_results::<i32>(c)?;

        let new_parents: Vec<_> = parents.into_iter().zip(children)
            .flat_map(|(parents, child)| {
                parents.into_iter().map(move |parent| {
                    NewParent { parent, child }
                })
            })
            .collect();

        insert_into(parents::versions_parents)
            .values(new_parents)
            .execute(c)?;

        Ok::<_, diesel::result::Error>(())
    })
        .expect("Failed to save successors")
}