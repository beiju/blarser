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

The gist is this:
* Start with a copy of the initial Blaseball state. To start with this will be
  the state of the `teams` and `players` collections at the start of the 
  Expansion era, as well as an empty set of `games`, and once that works I will
  expand backwards, to Discipline, and outwards, to other entity types.
* Iterate over the merged streams of feed events, player and team updates, 
  and game events, in chronological order.
* When a feed event is encountered, generate a new copy of the entire state 
  (with heavy use of data sharing, otherwise the memory usage will be extreme)
  that reflects the changes. When the changes are knowable, like "this event 
  causes the current batter to be set to <id>", record the new data directly.
  Otherwise, record a placeholder that represents what is known (for example,
  after a party the player's `divinity` should be between 0.04 and 0.08 higher 
  than the previous value). Store this copy associated with the event.
* When a chron update is encountered, verify the current state against the chron
  update. For every concrete piece of state, just assert that it matches the 
  chron record. For every placeholder piece of state:
  * If the chron record doesn't obey the bounds, treat it the same as a 
    concrete value that didn't match.
  * If the placeholder's predecessor is a concrete value, then the change can be
    easily computed as the difference between the predecessor value and the 
    value from the chron record. Replace the placeholder in-place (place place 
    place) with the computed change. The data should be stored in such a way 
    that this action mutates the state associated with the event that generated
    the placeholder, making it appear like we always knew the exact change that
    the event caused.
  * If the placeholder's predecessor is also a placeholder, the value is 
    unknowable. Use the chron record to refine the bounds of the entire chain, 
    but otherwise leave the two or more placeholders intact.
* Somehow transform this dense data structure full of internal references into
  a consumable API.

Some complicating factors, and how I plan to handle them:
* Many changes don't have an associated Feed event. For example, the generation
  of the initial schedule, or instances of the devs making manual changes. I 
  plan to handle that with a mix of manually-added events and rules that allow 
  certain types of Chron update to create events upon receiving the update.
* The dates associated with Chron updates represent approximately when the HTTP 
  request was sent to the Blaseball API and not when the database query was 
  made. As such, a chron record might come in with a timestamp indicating it's 
  after a particular event the data represents a state before that event. This 
  would cause a validation failure in the best case and an erroneously-filled 
  placeholder in the worst case. I plan to handle it by brute force, testing the
  chron record against every state since the earliest-possible placement of
  the previous chron record for that entity type (or possibly for that entity,
  with a reasonable bound in that case because individual entity updates can 
  be far apart). This should yield a set of placements that pass validation. If 
  no placements pass validation, that's an error. I'm not sure if it's possible
  for multiple placements to pass validation with different solutions to the 
  placeholder values, but I plan to detect that and make it an error as well. 