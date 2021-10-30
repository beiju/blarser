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
* Run hypothetical scenarios, such as what a particular season would've looked 
  like without a particular rule (e.g. "what if the Crabs hadn't had the 
  fourth strike in Season 13")

Approach
--------

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