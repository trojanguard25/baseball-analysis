class AtBat():
    bbos_atbat_id = ""

    # plate appearance scene info
    batter_id = ""
    pitcher_id = ""
    pitcher_side = ""
    batter_side = ""
    stadium_id = ""
    league_id = ""
    level_id = ""
    batter_pitcher_age = 0.0
    on_first = False
    on_second = False
    on_third = False

    # at bat conclusion info
    contact = False
    bunt = False
    hit = False
    walk = False
    hit_by_pitch = False
    strikeout = False
    strikeout_looking = False
    strikeout_swinging = False
    double = False
    triple = False
    homerun = False
    error = False
    fielder_choice = False
    
    # ball in play info
    groundball = False
    flyball = False
    
    # pitches
    pitches = 0.0
    balls = 0.0
    strikes = 0.0
    strikes_swing = 0.0
    strikes_looking = 0.0
    strikes_foul = 0.0
    
    Process(self, atbat_id, conn):
        # query atbat info
        # query pitches info
        # query batter info
        # query pitcher info


class AtBats():
    PA = 0.0
    AB = 0.0
    H = 0.0
    2B = 0.0
    3B = 0.0
    HR = 0.0
    TB = 0.0
    SO = 0.0
    BB = 0.0
    R = 0.0
    SB = 0.0
    AVG = 0.0
    OBP = 0.0
    SLG = 0.0
    BABIP = 0.0
    ISO = 0.0
    IDO = 0.0
    
    # matchup info
    batter_pitcher_age = 0.0
    batter_left = 0.0
    batter_right = 0.0
    pitcher_left = 0.0
    pitcher_right = 0.0
    
    # pitch info
    pitches = 0.0
    strikes_swinging = 0.0
    strikes_looking = 0.0
    strikes_foul = 0.0
    balls = 0.0
    
    # ball in play info
    contact = 0.0
    groundballs = 0.0
    flyballs = 0.0
    
    stadiums = {}
    pitchers = {}
    leagues = {}
    
    atbats = {}
    

    def AddAtBat(self, atbat, recalculate=True):
        self.atbats[atbat.atbat_id] = atbat
        if recalculate:
            self.Recalculate()
        
    def CombineAtBats(self, atbats):
        for ab in atbats:
            self.atbats[ab.atbat_id] = ab
            
        self.Recalculate()
        
    def Recalculate(self): 