blarser
=======

the name is bad

Motivation
----------

Blarser exists for a few reasons. First, there are several primary sources for 
Blaseball data, including the Feed (for seasons where it exists) and the
data that Chronicler captures. There are relationships between these data
streams. The most obvious is that (approximately) all game updates have an
associated feed event, for seasons where the feed existed, but there are also
connections between feed events and other chron types and between two chron
types. Finding these connections is not straightforward, since there is not a
1-to-1 relationship. Blarser aims to find those connections and expose them to 
other tools.

Second, there is information that can only be derived by considering multiple
events together. One example comes up when considering the task of 
reconstructing missing game events from the feed: There is some information 
missing from feed events, such as baserunner advancement and, surprisingly, 
player IDs of "ground out" type events. In some cases, baserunner advancement
can be derived from later events (for example, if a player tries to steal 
second they must have been on first). And in most if not all cases, player IDs 
for outs can be derived by looking at the surrounding game events. Blarser 
aims to make that data available directly in the relevant event.

Third, there is information that can only be derived by parsing text. I'm sure 
there's a good example but right now I can only think of things that you could
*technically* derive from other sources, like whether a skipped at-bat was due 
to Shelled or Elsewhere, but which it's simpler to get by parsing. More 
importantly, parsing the text offers a way of double-checking assumptions that 
might have otherwise caused incorrect data -- it was once assumed that a giant
peanut crashing into the field was just flavor text. Properly parsing text is 
hard to get right and easy to get wrong (just ask SIBR about Stout Schmitt). 
Blarser aims to do it right and make the results available to other tools.

Example uses
------------

Blarser will have done its job if, among other things, it can be used to:

* Reconstruct missing game updates, including the information that's not 
  available in the feed
* Reconstruct the feed, for seasons before the feed existed
* Tell you the sources of every change to the objects Chron records
* Provide the source data for the datablase
* Generate expected game updates for the unseen Gamma seasons (which probably
  can't be fully determined, at least for baserunner positions, but you could
  represent the uncertainty)
* Run hypothetical scenarios, such as what a particular season would've looked 
  like without a particular rule (e.g. "what if the Crabs hadn't had the 
  fourth strike in Season 13")

Approach
--------

double disclaimer on top of the following disclaimer: I reworked the plan, 
again. see "Update: Revised ingest plan" for details. keeping this section 
because a lot of it still applies.

First, a disclaimer: This is just a plan. Blarser has gone through several 
prototypes where I learned lessons about what will and will not work. This is 
what I think will work based on those lessons, but almost nothing been 
implemented yet.

### Data structure
* `BlaseballState` is the parent. It contains every object in Blaseball and 
  there will be a copy of it for every feed event (and more). Obviously, it 
  will need some heavy data sharing to not explode in size. `BlaseballState` is
  treated as immutable with a major exception: it can contain placeholders which
  will later be mutated to concrete values when the correct value becomes known.
* `BlaseballState` contains a `RecordSet` for each of `players`, `teams`, etc. 
  (eventually almost all the entities Chron captures, although starting with 
  those two) and a `RecordSet` of current `games` (which Chron treats 
  differently). 
* Each `RecordSet` consists of `Record`s. A `Record` represents a Blaseball 
  entity (as in player, team, etc. not as in a god or other NPC).
* Each `Record` has a `Uuid` and a list of `Property`s. There should be a 
  `Property` for each property the corresponding Chron entity has.
* Each `Property` tracks its `predecessor`, the event that it was `caused_by` 
  (the event that caused the property to change to its current value, which may 
  be a feed event or a different type of event, \[see "Complicating 
  Factors"\]), the Chron update that it was first `percieved_by`, and its 
  contained `value`.
* A property's value may be `Concrete` values or may be various types of unknown
  values. These types of unknown values encode the known bounds of what the 
  value might be.

### Logic
* Start with the initial `BlaseballState`. To start with this will be the state
  of the `teams` and `players` at the start of the Expansion era, all with 
  Confirmed values, as well as an empty set of `games`. Once that works I will 
  expand backwards, to Discipline, and outwards, to other entity types.
* Iterate over the merged streams of feed events, player and team updates, 
  and game events, in chronological order.
* When a feed event is encountered, generate a successor `BlaseballState` that
  reflects the changes. When the changes are knowable, like "this event causes
  the current batter to be set to <id>", record the new data as concrete values.
  Otherwise, record unknown values with whatever bounds are possible (for 
  example, after a party the player's `divinity` should be between 0.04 and 
  0.08 higher than the previous value). This is where the parsing happens, and
  the raw event, parsed event, and `BlaseballState` are stored together.
* When a chron update is encountered, apply it to the `Record` for the 
  entity it represents:
  * If the list of property names doesn't match the chron update, return 
    `FailedValidation`.
  * Otherwise, apply it to every `Property` of the `Record`:
    * If the property's value doesn't match the chron update, either by a 
      `ConcreteValue` that's not equal or a bounded unknown value where the 
      chron update doesn't meet the bounds, return `FailedValidation`.
    * If the property's value is unknown, and the bounds are met, modify the 
      value to be known. If the property's predecessor is also unknown, apply 
      whatever information we can to it. I can only think of instances where it
      could be used to tighten the bounds, but if there are instances where it 
      makes the predecessor's value knowable then update that as well. Continue
      this process up the chain of predecessors until a known value is reached.
* Somehow transform this dense data structure full of internal references into
  a consumable API.

### Complicating Factors
* Many changes don't have an associated Feed event. For example, the generation
  of the initial schedule, or instances of the devs making manual changes. I 
  plan to handle that with a mix of manually-added events and rules that allow 
  certain types of Chron update to create events upon receiving the update.
* The dates associated with Chron updates represent approximately when the HTTP 
  request was sent to the Blaseball API and not when the database query was 
  made. As such, a chron update might come in with a timestamp indicating it's 
  after a particular event the data represents a state before that event. This 
  would cause a validation failure in the best case and an 
  erroneously-concretized placeholder in the worst case. I plan to handle this
  by brute force, applying the update to every `BlaseballState` since the 
  earliest state that passed validation for the previous chron update. The 
  validation function will return a list of values that were concretized by 
  that validation, and my first implementation will just assert that all states
  that validate cause identical concretizations. If that assert ever fails, my 
  backup plan is multiple timelines. Just keep a list of parallel 
  `BlaseballState`s representing multiple timelines. Apply each feed and 
  chron update to all of them, and whenever a Chron update completely fails 
  validation on a timeline, discard the timeline. Hopefully all timelines but 
  one will quickly die out. If not, they probably converge, and I'll have to 
  merge them somehow. Ugh.
* This is a hefty operation, and running it from start to finish might require
  a prohibitively high amount of RAM and/or CPU. Luckily, there is a point at
  which the running code no longer needs a given `BlaseballState`. At that point
  the state can't be mutated, so it's "finished", and will never be accessed by
  the ingester again, so can be freed from memory. A process can chew up the end
  of the `BlaseballState` chain (linked list or whatever data structure it ends
  up being) and serialize it, as long as the serialization can correctly 
  serialize internal pointers. It only has to stop serializing once it 
  encounters any unconfirmed value. Then the process can be resumed by loading
  any serialized state and restarting the feed and chron queries from the 
  timestamp of the latest event, since those are all ingested in strictly 
  chronological order. The same chew-up-the-end idea could be used for whatever
  code generates the data that will feed the eventual API responses, because 
  the `BlaseballState` object is definitely the wrong form to use for answering 
  queries.
* Sometimes, one possible resolution of an unknown value is that it didn't 
  change. In that case, you might not get an update for it. I'm not sure if this
  will be an issue because the only way I can think of to get a "maybe changed, 
  maybe not" situation is baserunner advancement, and stream events get picked 
  up more or less continuously while a game is going. However, if it happens, I 
  think it's reasonable to handle it by keeping track of last update for each 
  Chron entity, and when you get an update for id X, look up when the last 
  update was for that ID and expire all unknown changes from before that time. 
  The reasoning here is that once you've seen two updates for a given ID, you
  would definitely have seen any update for another ID. Oh wait, unless the 
  iteration order isn't stable. Maybe it should be 3 updates.

### Update: Revised ingest plan

\[NOTE this is also out of date. Read on\]

Revised the whole ingest plan because the handling of chron update timestamp 
fuzziness wasn't robust enough, and the previous plan wasn't database-friendly.

1. One task ingests Feed events. Store feed events in a table and a list of 
   which entities are (or may be) changed by that event in a linked table. I 
   will probably limit this task so it stays at most ~1hr ahead of the Chron 
   task, just so it's not crunching my computer every time I restart during 
   development, and it doesn't hurt to keep that in prod even though it's not
   necessary for any functionality.
3. Another task ingests Chron updates. Each Chron update has a time range that 
   could be the true time for the update, which should be derived from the 
   `validFrom` timestamp (which is only an approximation). When each is 
   ingested, apply this resolution algorithm:
   1. Wait until the feed ingest task has caught up to the end of the time 
      range. I requested for Eventually to return the timestamp of its last 
      successful ingest with every response and allie said they are willing to
      add it.
   2. Fetch the latest *resolved* chron update for this entity. *Resolved* 
      means that the update's order with respect to all feed events that can 
      affect it is precisely known. The starting state (the state Blarser 
      fetches when it first starts up) is assumed to be resolved. Use this to 
      initialize an object that can represent partial information about the 
      entity. This may be the time to impose the Rust type system onto entity
      data.
   3. Walk forward through the relevant events, changing the entity data
      according to each one. "Events" includes feed events and timed events, 
      which are events like "start the season" that occur at specific times 
      which can be mined from the entity data. Once the timestamp of the events 
      gets into the range for this Chron update, start validating this chron 
      update's data against the computed entity data we get from walking the
      events. Record every valid placement in one list and record the mismatched 
      fields in another list. Stop once the end of the time range for this 
      update is reached. (If there's an event exactly at the end of the range, 
      it's important to stop *before* processing that feed event.)
   4. If there are zero valid placements, this is a validation error. Need to
      have a think about what to do with a validation error. Presumably it 
      should be displayed somewhere, and this is where the mismatched fields 
      list comes in.
   5. If there are two or more valid placements, then this can't be placed. 
      It still gets saved to the database (and its time range is one of the 
      fields that gets stored) but not marked as resolved.
   6. If there is exactly one valid placement, that is the correct placement.
      Mark this chron record as *resolved*. Set the beginning of its time range
      to the timestamp of the last event that caused a change, if that event is
      inside the existing time range. Set the end of the previous update's time
      range to the timestamp of the last event that caused a change, if that 
      event is inside the existing time range. Note that this may not be the 
      same update as the previous *resolved* update! Finally, if the previous
      update was not resolved, apply the resolution process again using the 
      updated end-of-range. Apply this recursively until you fail to resolve an
      update or until you hit an already-resolved update.
   
### Update update: Revised ingest plan 2

The previous ingest plan doesn't work well with cached values and the story for
real-time ingest is muddy. New plan: 

- Initialize: Pull the entire Chron state at time T (user-selected, should 
  be in the middle of a period of Blaseball dormancy so the state is 
  definitely faithful to the site and not affected by network delay or 
  caching). Convert that state to a fully-known `PartialInformation` (this 
  is the only time a `PartialInformationRaw` should be directly converted 
  into a `PartialInformation`).
- Pull Feed: Concurrently with Chron, pull the Feed. The Feed stream yields 
  tuples of (ingest_time, Option<event>). The timestamp of an item is the 
  event's timestamp if there is an event, otherwise the ingest_time.
  1. Loop:
     1. If there is a next timed event, apply it. Otherwise break.
  2. If there is an event:
     1. Store it in the DB for later
     2. Apply it
  
  "Applying" an event, timed or feed, means to run through the effects of 
  the event on every currently-possible version of the entities it affects. 
  When an event modifies or observes an entity, it must fetch all currently-
  possible versions of that entity and apply the changes to all of them. For 
  each version of each entity, it must produce either:
  - No change - this entity state was definitely not changed by this event. No 
    action is taken on this entity in the database.
  - Incompatible - the event could not be applied to this entity state. This 
    possible state should be marked as terminated, meaning it will no longer 
    be included in the list of every currently-possible version of this 
    entity. One example of "incompatible" is if this state has a player on 
    3rd, a single is hit, and the player on 3rd does not score. This means 
    that it was incorrect to put the player on 3rd.
  - Change(s) - the entity could be in one of these possible states. There 
    could be one successor, if the effects are fully known, or multiple 
    successors if there were multiple possible outcomes.
  
  If all versions lead to Incompatible that is a fatal error. Otherwise, 
  identical successors should be merged (easier said than done) and all 
  successors should be saved to the database. Some care will have to be 
  taken to ensure "maybe modified, maybe not" is handled correctly.
- Pull Chron: Concurrently with the Feed, pull Chronicler versions. For each 
  new version seen:
  - Split it by cache boundaries. These should be encoded into the type 
    using the `PartialInformation` derive macro. Each cache boundary, 
    including the top level, has a range of times in which it could be valid.
    (This range could be zero, e.g. for game updates where lastUpdateFull 
    contains a time stamp.)
  - Shrink the time ranges according to existing information: The time range 
    cannot overlap with the time range of any other *resolved* update. Get 
    resolved updates nearby and shrink the time range accordingly. I think 
    there should never be discontinuities. Hopefully.
  - Separately for each cache boundary: Get all the versions that are valid 
    as of the end of the range. For each of those, get its chain of 
    predecessors up to the start of the range. Diff the observation against 
    every version in the chain. There are two properties a diff can have: it is
    "empty" if contains no difference at all, and it is "compatible" if 
    every difference it contains is going from an unknown or partially-known 
    value to a known value (which is compatible with the partial knowledge, 
    if applicable).
    - If the diff is not compatible with any version in the chain, the chain is 
      invalid. Terminate the *last* version in the chain and any descendants 
      it has. If this is the last non-terminated descendant of some other 
      version, terminate that version as well, recursively.
    - If the diff is compatible with a single version in the chain, then this 
      observation is now *resolved*. Shrink its time range to be the time 
      range of the single version to which it applied (if smaller) and store 
      it as resolved for this chain (and descendants) only. I don't yet know 
      how to format that information, but it's important that it not be 
      magically resolved for all chains. If there is an unresolved version 
      before or after this one, and this one overlaps its time range, try to 
      resolve it again (run the whole process as if it was just observed) now 
      that its time range has shrunk.
      - If the diff is non-empty, apply the changes to this version and 
        propagate them forward. I think the best way to propagate them 
        forward is to just throw away all descendants and compute them again 
        from the stored feed events.
    - If the diff is compatible with multiple versions, store this observation 
      for later resolution.
  