this is just a scratchpad for my thoughts. not considered documentation

each entity has its own separate time stream. it may be possible to use 
information from one entity to figure things out about another entity but i'm 
not designing for that and i don't know where it would even be useful

an entity's time stream is composed of generations. each generation represents 
the state of that entity for a range of time. the generation includes:
- the time it started
- the event which created it (Start, Feed, Timed, or Manual)

feed events table has:
- just feed event data, i think

within a generation there are >=1 versions. each version represents a possible
state for that entity at that generation time. a version includes:
- its generation
- the entity state
- the versions from which it was descended. these should all be versions from
  the previous generation. multiple parents represents when the event takes two
  versions with different state and transitions them to successor versions that
  have the same state.
- a map of event piece identifier -> "has pending event" boolean

to run event ingest until a given time:
- fetch the next feed event before given time
- if there is one:
  - apply timed events upto event time
  - save event data in the feed events table
  - apply event with a StateInterface. for each entity it alters:
    - fetch the latest generation for that entity and all of its versions
    - assert that the latest generation is before event time, just in case
    - run the update function to get the successor versions, combining duplicates
    - add a new generation with all the versions
  - repeat event ingest
- if there is no next feed event before given time:
  - apply timed events upto given time
  - end

to ingest a chron update:
- for each cached piece:
  - compute the range of possible placements
  - run event ingest until the end of the time range
  - fetch the current generation at the beginning of the time range
  - fetch events associated with all following generations
  - for each version in the generation:
    - if the "has pending event" boolean is true, skip (this ensures that 
      updates are applied in the order they were observed)
    - attempt to apply the event to the version
    - if event was applied successfully
      - compute entire successor tree using fetched events
      - merge with previous successor trees (deduping, etc.)
    - otherwise
      - store the failure reason somewhere (tbd) for later display
    - 

