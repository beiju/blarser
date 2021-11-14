use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_repr::Deserialize_repr;

#[derive(Deserialize)]
pub struct EventuallyResponse(pub(crate) Vec<EventuallyEvent>);

impl EventuallyResponse {
    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }
}

impl IntoIterator for EventuallyResponse {
    type Item = EventuallyEvent;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct EventuallyEvent {
    pub id: String,
    pub created: DateTime<Utc>,
    pub r#type: EventType,
    pub category: i32,
    pub description: String,
}

#[derive(Deserialize_repr, PartialEq, Debug)]
#[repr(i32)]
pub enum EventType {
    Undefined = -1,
    LetsGo = 0,
    PlayBall = 1,
    HalfInning = 2,
    PitcherChange = 3,
    StolenBase = 4,
    Walk = 5,
    Strikeout = 6,
    FlyOut = 7,
    GroundOut = 8,
    HomeRun = 9,
    Hit = 10,
    GameEnd = 11,
    BatterUp = 12,
    Strike = 13,
    Ball = 14,
    FoulBall = 15,
    SolarPanelsOverflow = 20,
    HomeFieldAdvantage = 21,
    HitByPitch = 22,
    BatterSkipped = 23,
    Party = 24,
    StrikeZapped = 25,
    WeatherChange = 26,
    MildPitch = 27,
    InningEnd = 28,
    BigDeal = 29,
    BlackHole = 30,
    Sun2 = 31,
    BirdsCircle = 33,
    FriendOfCrows = 34,
    BirdsUnshell = 35,
    BecomeTripleThreat = 36,
    GainFreeRefill = 37,
    CoffeeBean = 39,
    FeedbackBlocked = 40,
    FeedbackSwap = 41,
    SuperallergicReaction = 45,
    AllergicReaction = 47,
    ReverbBestowsReverberating = 48,
    ReverbRosterShuffle = 49,
    Blooddrain = 51,
    BlooddrainSiphon = 52,
    BlooddrainBlocked = 53,
    Incineration = 54,
    IncinerationBlocked = 55,
    FlagPlanted = 56,
    RenovationBuilt = 57,
    LightSwitchToggled = 58,
    DecreePassed = 59,
    BlessingOrGiftWon = 60,
    WillRecieved = 61,
    FloodingSwept = 62,
    SalmonSwim = 63,
    PolarityShift = 64,
    EnterSecretBase = 65,
    ExitSecretBase = 66,
    ConsumersAttack = 67,
    EchoChamber = 69,
    GrindRail = 70,
    TunnelsUsed = 71,
    PeanutMister = 72,
    PeanutFlavorText = 73,
    TasteTheInfinite = 74,
    EventHorizonActivation = 76,
    EventHorizonAwaits = 77,
    SolarPanelsAwait = 78,
    SolarPanelsActivation = 79,
    TarotReading = 81,
    EmergencyAlert = 82,
    ReturnFromElsewhere = 83,
    OverUnder = 85,
    UnderOver = 86,
    Undersea = 88,
    Homebody = 91,
    Superyummy = 92,
    Perk = 93,
    Earlbird = 96,
    LateToTheParty = 97,
    ShameDonor = 99,
    AddedMod = 106,
    RemovedMod = 107,
    ModExpires = 108,
    PlayerAddedToTeam = 109,
    PlayerReplacedByNecromancy = 110,
    PlayerReplacesReturned = 111,
    PlayerRemovedFromTeam = 112,
    PlayerTraded = 113,
    PlayerSwap = 114,
    // TODO What does 115 mean?
    PlayerBornFromIncineration = 116,
    PlayerStatIncrease = 117,
    PlayerStatDecrease = 118,
    PlayerStatReroll = 119,
    PlayerStatDecreaseFromSuperallergic = 122,
    PlayerMoveFailedForce = 124,
    EnterHallOfFlame = 125,
    ExitHallOfFlame = 126,
    PlayerGainedItem = 127,
    PlayerLostItem = 128,
    ReverbFullShuffle = 130,
    ReverbLineupShuffle = 131,
    ReverbRotationShuffle = 132,
    // At this point I got bored typing them all and only filled in the ones I encountered
    AddedModFromOtherMod = 146,
    RunsScored = 209,
}