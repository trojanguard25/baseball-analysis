use mlb;

select a.atbatID, a.gameID, a.batterID, a.pitcherID,
    g.leagueLevel, g.stadiumID, g.gameYear, g.gameDate 
    from atbats a join games g on a.gameID = g.gameID 
    where g.gameType='regular' 
    order by a.batterID, g.gameYear, g.leagueLevel, g.gameDate;


