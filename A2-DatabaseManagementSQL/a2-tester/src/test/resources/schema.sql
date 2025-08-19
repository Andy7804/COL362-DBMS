-- Create season table
CREATE TABLE season (
    season_id VARCHAR(20) PRIMARY KEY,
    year SMALLINT NOT NULL CHECK (year BETWEEN 1900 AND 2025),  -- Check constraint for valid year
    start_date DATE NOT NULL,
    end_date DATE NOT NULL
);
/* Constraints explained:
   - Primary Key: season_id ensures unique identification
   - Check constraint on year ensures valid season years
   - NOT NULL ensures required fields are always provided
*/

-- Create team table
CREATE TABLE team (
    team_id VARCHAR(20) PRIMARY KEY,
    team_name VARCHAR(255) NOT NULL UNIQUE,  -- Unique constraint added
    coach_name VARCHAR(255) NOT NULL,
    region VARCHAR(20) NOT NULL UNIQUE  -- Unique constraint added
);
/* Constraints explained:
   - Primary Key: team_id ensures unique identification
   - UNIQUE on team_name prevents duplicate team names
   - UNIQUE on region prevents multiple teams in same region
   - NOT NULL ensures required fields are always provided
*/

-- Create player table
CREATE TABLE player (
    player_id VARCHAR(20) PRIMARY KEY,
    player_name VARCHAR(255) NOT NULL,
    dob DATE NOT NULL CHECK (dob < '2016-01-01'),  -- Check constraint for valid date of birth
    batting_hand VARCHAR(20) NOT NULL CHECK (batting_hand IN ('left', 'right')),  -- Check constraint for valid batting hand
    bowling_skill VARCHAR(20) CHECK (bowling_skill IN ('fast', 'medium', 'legspin', 'offspin')),  -- Check constraint for valid bowling skill
    country_name VARCHAR(20) NOT NULL
);
/* Constraints explained:
   - Primary Key: player_id ensures unique identification
   - Check constraints ensure valid values for batting_hand and bowling_skill
   - Check constraint on dob ensures valid birth date
   - NOT NULL ensures required fields are always provided
*/

-- Create match table
CREATE TABLE match (
    match_id VARCHAR(20) PRIMARY KEY,
    match_type VARCHAR(20) NOT NULL CHECK (match_type IN ('league', 'playoff', 'knockout')),  -- Check constraint for match type
    venue VARCHAR(20) NOT NULL,
    team_1_id VARCHAR(20) NOT NULL,
    team_2_id VARCHAR(20) NOT NULL,
    match_date DATE NOT NULL,
    season_id VARCHAR(20) NOT NULL,
    win_run_margin SMALLINT,
    win_by_wickets SMALLINT,
    win_type VARCHAR(20) CHECK (win_type IN ('runs', 'wickets', 'draw')),  -- Check constraint for win type
    toss_winner SMALLINT CHECK (toss_winner IN (1, 2)),  -- Check constraint for toss winner
    toss_decide VARCHAR(20) CHECK (toss_decide IN ('bowl', 'bat')),  -- Check constraint for toss decision
    winner_team_id VARCHAR(20),
    FOREIGN KEY (team_1_id) REFERENCES team(team_id),
    FOREIGN KEY (team_2_id) REFERENCES team(team_id),
    FOREIGN KEY (season_id) REFERENCES season(season_id) ON DELETE CASCADE,
    FOREIGN KEY (winner_team_id) REFERENCES team(team_id),
    FOREIGN KEY (venue) REFERENCES team(region)
);
/* Constraints explained:
   - Primary Key: match_id ensures unique identification
   - Foreign Keys maintain referential integrity with team and season tables
   - Check constraints ensure valid values for match_type, win_type, toss_winner, and toss_decide
   - NOT NULL ensures required fields are always provided
*/

-- Create auction table
CREATE TABLE auction (
    auction_id VARCHAR(20) PRIMARY KEY,
    season_id VARCHAR(20) NOT NULL,
    player_id VARCHAR(20) NOT NULL,
    base_price BIGINT NOT NULL CHECK (base_price >= 1000000),  -- Check constraint for minimum base price
    sold_price BIGINT,
    is_sold BOOLEAN NOT NULL,
    team_id VARCHAR(20),
    UNIQUE (player_id, team_id, season_id),  -- Unique constraint added
    FOREIGN KEY (season_id) REFERENCES season(season_id) ON DELETE CASCADE,
    FOREIGN KEY (player_id) REFERENCES player(player_id),
    FOREIGN KEY (team_id) REFERENCES team(team_id)
);
/* Constraints explained:
   - Primary Key: auction_id ensures unique identification
   - Unique constraint prevents duplicate player-team-season combinations
   - Check constraint ensures minimum base price
   - Foreign Keys maintain referential integrity
   - NOT NULL ensures required fields are always provided
*/

-- Create player_team table
CREATE TABLE player_team (
    player_id VARCHAR(20),
    team_id VARCHAR(20),
    season_id VARCHAR(20),
    PRIMARY KEY (player_id, team_id, season_id),
    FOREIGN KEY (player_id, team_id, season_id) 
        REFERENCES auction(player_id, team_id, season_id) -- Composite foreign key
            ON DELETE CASCADE  
);
/* Constraints explained:
   - Composite Primary Key ensures unique player-team-season combinations
   - Composite Foreign Key maintains referential integrity with auction table
   This is a crucial composite foreign key relationship that ensures players can only be
   in teams they were auctioned to for specific seasons
*/

-- Create balls table
CREATE TABLE balls (
    match_id VARCHAR(20),
    innings_num SMALLINT,
    over_num SMALLINT,
    ball_num SMALLINT,
    striker_id VARCHAR(20) NOT NULL,
    non_striker_id VARCHAR(20) NOT NULL,
    bowler_id VARCHAR(20) NOT NULL,
    PRIMARY KEY (match_id, innings_num, over_num, ball_num),
    FOREIGN KEY (match_id) REFERENCES match(match_id) ON DELETE CASCADE,
    FOREIGN KEY (striker_id) REFERENCES player(player_id),
    FOREIGN KEY (non_striker_id) REFERENCES player(player_id),
    FOREIGN KEY (bowler_id) REFERENCES player(player_id)
);
/* Constraints explained:
   - Composite Primary Key ensures unique identification of each ball
   - Foreign Keys maintain referential integrity
   - NOT NULL ensures required fields are always provided
   This table serves as the parent table for batter_score, wickets, and extras tables
*/

-- Create batter_score table
CREATE TABLE batter_score (
    match_id VARCHAR(20),
    over_num SMALLINT,
    innings_num SMALLINT,
    ball_num SMALLINT,
    run_scored SMALLINT NOT NULL CHECK (run_scored >= 0),  -- Check constraint for positive runs
    type_run VARCHAR(20) CHECK (type_run IN ('running', 'boundary')),  -- Check constraint for run type
    PRIMARY KEY (match_id, innings_num, over_num, ball_num),
    FOREIGN KEY (match_id) REFERENCES match(match_id) ON DELETE CASCADE,
    CONSTRAINT fk_balls FOREIGN KEY (match_id, innings_num, over_num, ball_num) 
        REFERENCES balls(match_id, innings_num, over_num, ball_num)
            ON DELETE CASCADE
);
/* Constraints explained:
   - Composite Primary Key matches the parent balls table
   - Composite Foreign Key maintains referential integrity with balls table
   - Check constraints ensure valid run values and types
   - NOT NULL ensures required fields are always provided
*/

-- Create wickets table
CREATE TABLE wickets (
    match_id VARCHAR(20),
    innings_num SMALLINT,
    over_num SMALLINT,
    ball_num SMALLINT,
    player_out_id VARCHAR(20) NOT NULL,
    kind_out VARCHAR(20) NOT NULL CHECK (kind_out IN ('bowled', 'caught', 'lbw', 'runout', 'stumped', 'hitwicket')),  -- Check constraint for dismissal type
    fielder_id VARCHAR(20),
    PRIMARY KEY (match_id, innings_num, over_num, ball_num),
    FOREIGN KEY (match_id) REFERENCES match(match_id) ON DELETE CASCADE,
    FOREIGN KEY (player_out_id) REFERENCES player(player_id),
    FOREIGN KEY (fielder_id) REFERENCES player(player_id),
    CONSTRAINT fk_balls FOREIGN KEY (match_id, innings_num, over_num, ball_num) 
        REFERENCES balls(match_id, innings_num, over_num, ball_num)
            ON DELETE CASCADE
);
/* Constraints explained:
   - Composite Primary Key matches the parent balls table
   - Composite Foreign Key maintains referential integrity with balls table
   - Check constraint ensures valid dismissal types
   - Regular Foreign Keys maintain referential integrity with player table
   - NOT NULL ensures required fields are always provided
*/

-- Create extras table
CREATE TABLE extras (
    match_id VARCHAR(20),
    innings_num SMALLINT,
    over_num SMALLINT,
    ball_num SMALLINT,
    extra_runs SMALLINT NOT NULL CHECK (extra_runs >= 0),  -- Check constraint for positive extras
    extra_type VARCHAR(20) NOT NULL CHECK (extra_type IN ('no_ball', 'wide', 'byes', 'legbyes')),  -- Check constraint for extra types
    PRIMARY KEY (match_id, innings_num, over_num, ball_num),
    FOREIGN KEY (match_id) REFERENCES match(match_id) ON DELETE CASCADE,
    CONSTRAINT fk_balls FOREIGN KEY (match_id, innings_num, over_num, ball_num) 
        REFERENCES balls(match_id, innings_num, over_num, ball_num)
            ON DELETE CASCADE
);
/* Constraints explained:
   - Composite Primary Key matches the parent balls table
   - Composite Foreign Key maintains referential integrity with balls table
   - Check constraints ensure valid extra runs and types
   - NOT NULL ensures required fields are always provided
*/

-- Create player_match table
CREATE TABLE player_match (
    player_id VARCHAR(20),
    match_id VARCHAR(20),
    role VARCHAR(20) NOT NULL CHECK (role IN ('batter', 'bowler', 'allrounder', 'wicketkeeper')),  -- Check constraint for player role
    team_id VARCHAR(20) NOT NULL,
    is_extra BOOLEAN NOT NULL,
    PRIMARY KEY (player_id, match_id),
    FOREIGN KEY (player_id) REFERENCES player(player_id),
    FOREIGN KEY (match_id) REFERENCES match(match_id) ON DELETE CASCADE,
    FOREIGN KEY (team_id) REFERENCES team(team_id)
);
/* Constraints explained:
   - Composite Primary Key ensures unique player-match combinations
   - Foreign Keys maintain referential integrity
   - Check constraint ensures valid player roles
   - NOT NULL ensures required fields are always provided
*/

-- Create awards table
CREATE TABLE awards (
    match_id VARCHAR(20),
    award_type VARCHAR(20) CHECK (award_type IN ('orange_cap', 'purple_cap')),  -- Check constraint for award types
    player_id VARCHAR(20) NOT NULL,
    PRIMARY KEY (match_id, award_type),
    FOREIGN KEY (match_id) REFERENCES match(match_id) ON DELETE CASCADE,
    FOREIGN KEY (player_id) REFERENCES player(player_id)
);
/* Constraints explained:
   - Composite Primary Key ensures unique match-award combinations
   - Foreign Keys maintain referential integrity
   - Check constraint ensures valid award types
*/

/* Additional not NULL constraints for some tables */

/* match table */
/* trigger function for 'match' table */
CREATE OR REPLACE FUNCTION check_add_match_constraints()
RETURNS TRIGGER AS $$
BEGIN 
    IF NEW.win_type = 'draw' AND (NEW.win_run_margin IS NOT NULL OR NEW.win_by_wickets IS NOT NULL) THEN
        RAISE EXCEPTION 'null constraint violation: win margins must be NULL for draw matches';
    END IF;

    IF NEW.win_type != 'draw' AND 
       ((NEW.win_run_margin IS NOT NULL AND NEW.win_by_wickets IS NOT NULL) OR 
        (NEW.win_run_margin IS NULL AND NEW.win_by_wickets IS NULL)) THEN
        RAISE EXCEPTION 'null constraint violation: exactly one win margin must be specified for non-draw matches';
    END IF;

    IF NEW.win_type = 'runs' AND NEW.win_by_wickets IS NOT NULL THEN
        RAISE EXCEPTION 'null constraint violation: win_by_wickets must be NULL when win_type is runs';
    END IF;

    IF NEW.win_type = 'wickets' AND NEW.win_run_margin IS NOT NULL THEN
        RAISE EXCEPTION 'null constraint violation: win_run_margin must be NULL when win_type is wickets';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
/* use the trigger function to check constraints BEFORE adding */
CREATE TRIGGER add_match_constraints
BEFORE INSERT OR UPDATE ON match
FOR EACH ROW
EXECUTE FUNCTION check_add_match_constraints();

/* auction table */
CREATE OR REPLACE FUNCTION check_add_auction_constraints()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.is_sold = true THEN
        IF NEW.sold_price IS NULL OR NEW.team_id IS NULL THEN
            RAISE EXCEPTION 'null constraint violation: sold_price and team_id must not be NULL when is_sold is true';
        END IF;
        
        IF NEW.sold_price < NEW.base_price THEN
            RAISE EXCEPTION 'null constraint violation: sold_price must be greater than or equal to base_price';
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER add_auction_constraints
BEFORE INSERT OR UPDATE ON auction
FOR EACH ROW
EXECUTE FUNCTION check_add_auction_constraints();

/* wickets table */
CREATE OR REPLACE FUNCTION check_add_wickets_constraints()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.kind_out IN ('caught', 'runout', 'stumped') AND NEW.fielder_id IS NULL THEN
        RAISE EXCEPTION 'null constraint violation: fielder_id must not be NULL for caught, runout, or stumped dismissals';
    END IF;

    IF NEW.kind_out = 'stumped' THEN
        /* this query checks if there is atleast one wicketkeeper in the player_match table for the given match, whose player_id = NEW.fielder_id */
        /* I sort of think SELECT 1 is unnecessary due to previous constraints, but we'll see */
        IF NOT EXISTS (
            SELECT 1 
            FROM player_match 
            WHERE player_id = NEW.fielder_id 
            AND match_id = NEW.match_id 
            AND role = 'wicketkeeper'
        ) THEN
            RAISE EXCEPTION 'for stumped dismissal, fielder must be a wicketkeeper';
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER add_wickets_constraints
BEFORE INSERT OR UPDATE ON wickets
FOR EACH ROW
EXECUTE FUNCTION check_add_wickets_constraints();

/* Advanced Table Constraints */

/* player sold in auction gets added to the team */
CREATE OR REPLACE FUNCTION post_auction_assignment()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.is_sold = true THEN 
        INSERT INTO player_team (player_id, team_id, season_id) 
        VALUES (NEW.player_id, NEW.team_id, NEW.season_id);
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER add_player_to_team
AFTER INSERT ON auction
FOR EACH ROW
EXECUTE FUNCTION post_auction_assignment();

/* automatic season_id generation */
CREATE OR REPLACE FUNCTION generate_season_id()
RETURNS TRIGGER AS $$
BEGIN
    /* generate season_id */
    NEW.season_id :='IPL' || NEW.year;

    /* check if season_id already exists in that year */
    IF EXISTS (
        SELECT 1
        FROM season
        WHERE year = NEW.year AND season_id != NEW.season_id
    ) THEN
        RAISE EXCEPTION 'A season already exists for the given year';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER add_season_id
BEFORE INSERT ON season
FOR EACH ROW
EXECUTE FUNCTION generate_season_id();

/*NOTE: TEXT and VARCHAR(n) are compatible data types in PostgreSQL*/

/* match_id validation - have some doubts here (clarification needed) */
CREATE OR REPLACE FUNCTION validate_new_match_id()
RETURNS TRIGGER AS $$
DECLARE
    exp_seq_id TEXT;
    match_seq_num INT;
    max_seq_num INT;
    exp_seq_num TEXT;

BEGIN
    exp_seq_id := NEW.season_id;
    exp_seq_num := RIGHT(NEW.match_id, 3); /* last three digits of match sequence number */

    /* Check 1: match_id = season_id + match_num */
    IF NEW.match_id != (exp_seq_id || exp_seq_num) THEN
        RAISE EXCEPTION 'sequence of match id violated: match_id must start with season_id';
    END IF;

    -- Additional Check : Check if sequence portion is numeric and 3 digits
    IF NOT exp_seq_num ~ '^[0-9]{3}$' THEN
        RAISE EXCEPTION 'sequence of match id violated: sequence must be 3 digits';
    END IF;

    match_seq_num := CAST(exp_seq_num AS INT);

    /* get the maximum sequence number for the given season */
    SELECT COALESCE(MAX(RIGHT(match_id, 3)::INT),0) INTO max_seq_num
    FROM match
    WHERE season_id = NEW.season_id;

    /* Checks for INSERT - doubt, what if 1,2,3 inserted and 2 deleted: can we insert 2 again ? */
    IF TG_OP = 'INSERT' THEN
        IF match_seq_num != max_seq_num+1 THEN
            RAISE EXCEPTION 'sequence of match id violated: expected sequence % but got %', max_seq_num + 1, match_seq_num;
        END IF;
    END IF;

    /* Checks for UPDATE */
    /* This constraint enforces the condition, that INSERT has to be called before UPDATE */
    /* Don't know if this is required, am onfirming with TA*/
    IF TG_OP = 'UPDATE' THEN
        IF match_seq_num > max_seq_num THEN
            RAISE EXCEPTION 'sequence of match id violated: expected sequence % but got %', max_seq_num, match_seq_num;
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER validate_match_id
BEFORE INSERT OR UPDATE ON match
FOR EACH ROW
EXECUTE FUNCTION validate_new_match_id();

/* home and away matches - needs to be checked though */
CREATE OR REPLACE FUNCTION check_home_away_matches()
RETURNS TRIGGER AS $$
DECLARE
    team_1_region TEXT;
    team_2_region TEXT;
    home_team_id TEXT;
    num_home_matches_1 INT;
    num_home_matches_2 INT;

BEGIN
    SELECT region INTO team_1_region FROM team WHERE team_id = NEW.team_1_id;
    SELECT region INTO team_2_region FROM team WHERE team_id = NEW.team_2_id;

    /* enforce constraint for league matches */
    IF NEW.match_type = 'league' THEN
        -- Check 1: venue must be home ground of one of the teams
        IF NEW.venue != team_1_region AND NEW.venue != team_2_region THEN
            RAISE EXCEPTION 'league match must be played at home ground of one of the teams';
        END IF;
        -- Determine the home team
        IF NEW.venue = team_1_region THEN
            home_team_id := NEW.team_1_id;
        ELSE
            home_team_id := NEW.team_2_id;
        END IF;

        SELECT COUNT(DISTINCT match.match_id) INTO num_home_matches_1
        FROM match
        WHERE season_id = NEW.season_id AND match_type = 'league'
        AND venue = team_1_region
        AND ((team_1_id = NEW.team_1_id AND team_2_id = NEW.team_2_id) OR (team_1_id = NEW.team_2_id AND team_2_id = NEW.team_1_id))
        AND (
            CASE 
                WHEN TG_OP = 'UPDATE' THEN match_id != NEW.match_id
                ELSE true
            END
        ); 

        SELECT COUNT(DISTINCT match.match_id) INTO num_home_matches_2
        FROM match
        WHERE season_id = NEW.season_id AND match_type = 'league'
        AND venue = team_2_region
        AND ((team_1_id = NEW.team_1_id AND team_2_id = NEW.team_2_id) OR (team_1_id = NEW.team_2_id AND team_2_id = NEW.team_1_id))
        AND (
            CASE 
                WHEN TG_OP = 'UPDATE' THEN match_id != NEW.match_id
                ELSE true
            END
        ); 

        IF home_team_id = NEW.team_1_id AND num_home_matches_1 = 1 THEN
            RAISE EXCEPTION 'each team can play only one home match in a league against another team: team 1 has played at home v/s team 2';
        END IF;

        IF home_team_id = NEW.team_2_id AND num_home_matches_2 = 1 THEN
            RAISE EXCEPTION 'each team can play only one home match in a league against another team: team 2 has played at home v/s team 1';
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql; 

CREATE TRIGGER check_league_matches
BEFORE INSERT OR UPDATE ON match
FOR EACH ROW
EXECUTE FUNCTION check_home_away_matches();

/* limit on International Players per Team */
CREATE OR REPLACE FUNCTION check_intl_players_limit()
RETURNS TRIGGER AS $$
DECLARE
    international_count INT;
BEGIN
    IF NEW.is_sold = true AND NEW.team_id IS NOT NULL THEN
        IF TG_OP = 'UPDATE' THEN
            -- If team or season didn't change and player was already sold, no need to check
            IF OLD.team_id = NEW.team_id AND 
                OLD.season_id = NEW.season_id AND 
                OLD.is_sold = true THEN
                RETURN NEW;
            END IF;
        END IF;

        SELECT COUNT(DISTINCT p.player_id) INTO international_count
        FROM auction a
        JOIN player p ON a.player_id = p.player_id
        WHERE a.team_id = NEW.team_id
        AND a.season_id = NEW.season_id
        AND a.is_sold = true
        AND p.country_name != 'India'
        AND a.player_id != NEW.player_id;  -- Exclude the current player from count

        IF EXISTS (
            SELECT 1 
            FROM player 
            WHERE player_id = NEW.player_id 
            AND country_name != 'India'
        ) THEN
            international_count := international_count + 1;
        END IF;

        -- Check if the limit is exceeded
        IF international_count > 3 THEN
            RAISE EXCEPTION 'there could be atmost 3 international players per team per season';
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER check_intl_limit
BEFORE INSERT OR UPDATE ON auction
FOR EACH ROW
EXECUTE FUNCTION check_intl_players_limit();

/* trigger match winner and awards */
CREATE OR REPLACE FUNCTION update_match_winner_and_awards()
RETURNS TRIGGER AS $$
DECLARE 
    batting_team_id TEXT;
    bowling_team_id TEXT;
    most_runs RECORD;
    most_wickets RECORD;
BEGIN 
    -- Only proceed if win_type is being set or changed
    IF NEW.win_type IS NOT NULL AND (OLD.win_type IS NULL OR OLD.win_type != NEW.win_type) THEN
        -- Get the batting and bowling team ids
        IF NEW.toss_winner = 1 AND NEW.toss_decide = 'bat' THEN
            batting_team_id := NEW.team_1_id;
            bowling_team_id := NEW.team_2_id;
        ELSIF NEW.toss_winner = 2 AND NEW.toss_decide = 'bowl' THEN
            batting_team_id := NEW.team_1_id;
            bowling_team_id := NEW.team_2_id;
        ELSE
            batting_team_id := NEW.team_2_id;
            bowling_team_id := NEW.team_1_id;
        END IF;

        -- Update winner_team_id based on win_type
        IF NEW.win_type = 'wickets' THEN
            NEW.winner_team_id := bowling_team_id;
        ELSIF NEW.win_type = 'runs' THEN
            NEW.winner_team_id := batting_team_id;
        ELSE -- draw
            NEW.winner_team_id := NULL;
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_match_winner_and_awards()
RETURNS TRIGGER AS $$
DECLARE 
    batting_team_id TEXT;
    bowling_team_id TEXT;
    most_runs RECORD;
    most_wickets RECORD;
BEGIN 
    -- Only proceed if win_type is being set or changed
    IF NEW.win_type IS NOT NULL AND (OLD.win_type IS NULL OR OLD.win_type != NEW.win_type) THEN
        -- Get the batting and bowling team ids
        IF NEW.toss_winner = 1 AND NEW.toss_decide = 'bat' THEN
            batting_team_id := NEW.team_1_id;
            bowling_team_id := NEW.team_2_id;
        ELSIF NEW.toss_winner = 2 AND NEW.toss_decide = 'bowl' THEN
            batting_team_id := NEW.team_1_id;
            bowling_team_id := NEW.team_2_id;
        ELSE
            batting_team_id := NEW.team_2_id;
            bowling_team_id := NEW.team_1_id;
        END IF;

        -- Update winner_team_id based on win_type
        IF NEW.win_type = 'wickets' THEN
            NEW.winner_team_id := bowling_team_id;
        ELSIF NEW.win_type = 'runs' THEN
            NEW.winner_team_id := batting_team_id;
        ELSE -- draw
            NEW.winner_team_id := NULL;
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Use BEFORE trigger instead of AFTER
DROP TRIGGER IF EXISTS update_winner_and_awards ON match;
CREATE TRIGGER update_winner_and_awards
BEFORE INSERT OR UPDATE ON match
FOR EACH ROW
EXECUTE FUNCTION update_match_winner_and_awards();

-- Separate trigger for awards since they need to be inserted AFTER the match update
CREATE OR REPLACE FUNCTION update_match_awards()
RETURNS TRIGGER AS $$
DECLARE 
    most_runs RECORD;
    most_wickets RECORD;
BEGIN 
    -- Only proceed if win_type is being set or changed
    IF NEW.win_type IS NOT NULL AND (OLD.win_type IS NULL OR OLD.win_type != NEW.win_type) THEN
        -- Find player with most runs (orange cap)
        SELECT 
            b.striker_id,
            SUM(bs.run_scored) as total_runs
        INTO most_runs
        FROM balls b
        JOIN batter_score bs ON b.match_id = bs.match_id 
            AND b.innings_num = bs.innings_num 
            AND b.over_num = bs.over_num 
            AND b.ball_num = bs.ball_num
        WHERE b.match_id = NEW.match_id
        GROUP BY b.striker_id
        ORDER BY total_runs DESC, b.striker_id ASC
        LIMIT 1;

        -- Find player with most wickets (purple cap)
        SELECT 
            b.bowler_id,
            COUNT(*) as wicket_count
        INTO most_wickets
        FROM balls b
        JOIN wickets w ON b.match_id = w.match_id 
            AND b.innings_num = w.innings_num 
            AND b.over_num = w.over_num 
            AND b.ball_num = w.ball_num
        WHERE b.match_id = NEW.match_id
            AND w.kind_out IN ('bowled', 'caught', 'lbw', 'runout', 'stumped', 'hitwicket')
        GROUP BY b.bowler_id
        ORDER BY wicket_count DESC, b.bowler_id ASC
        LIMIT 1;

        -- Insert awards
        IF most_runs IS NOT NULL THEN
            INSERT INTO awards (match_id, award_type, player_id)
            VALUES (NEW.match_id, 'orange_cap', most_runs.striker_id);
        END IF;

        IF most_wickets IS NOT NULL THEN
            INSERT INTO awards (match_id, award_type, player_id)
            VALUES (NEW.match_id, 'purple_cap', most_wickets.bowler_id);
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_match_awards
AFTER INSERT OR UPDATE ON match
FOR EACH ROW
EXECUTE FUNCTION update_match_awards();

-- DELETE CONSTRAINTS ===========================================================

/* need to check, may need to remove cases in which player is a fielder */
/* trigger for auction_deletion - cases ehich are not covered by CASCADE delete */
CREATE OR REPLACE FUNCTION handle_auction_deletion()
RETURNS TRIGGER AS $$
BEGIN
    -- Only proceed if the auction was for a sold player
    IF OLD.is_sold = true THEN

        -- Delete from awards for matches in the season
        DELETE FROM awards 
        WHERE player_id = OLD.player_id 
        AND match_id IN (
            SELECT match_id 
            FROM match 
            WHERE season_id = OLD.season_id
        );

        -- Delete from player_match for matches in the season
        DELETE FROM player_match 
        WHERE player_id = OLD.player_id 
        AND match_id IN (
            SELECT match_id 
            FROM match 
            WHERE season_id = OLD.season_id
        );

        -- Delete from balls where player was involved
        DELETE FROM balls 
        WHERE (striker_id = OLD.player_id 
            OR non_striker_id = OLD.player_id 
            OR bowler_id = OLD.player_id)
        AND match_id IN (
            SELECT match_id 
            FROM match 
            WHERE season_id = OLD.season_id
        );

        -- Delete balls where the player was a fielder in a dismissal
        DELETE FROM balls
        WHERE EXISTS (
            SELECT 1
            FROM wickets
            WHERE wickets.match_id = balls.match_id
            AND wickets.innings_num = balls.innings_num
            AND wickets.over_num = balls.over_num
            AND wickets.ball_num = balls.ball_num
            AND wickets.fielder_id = OLD.player_id
        );
        
        -- Note: batter_score, extras, and wickets will be automatically deleted 
        -- due to the ON DELETE CASCADE constraint from balls table
    END IF;
    
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER auction_deletion
BEFORE DELETE ON auction
FOR EACH ROW
EXECUTE FUNCTION handle_auction_deletion();

-- VIEWS =====================================================================
/* batter_stats */
CREATE OR REPLACE VIEW batter_stats AS
WITH match_innings_runs AS (
    -- First get total runs per player per innings
    SELECT 
        b.striker_id as player_id,
        b.match_id,
        b.innings_num,
        COALESCE(SUM(bs.run_scored), 0) as innings_runs
    FROM balls b
    LEFT JOIN batter_score bs ON b.match_id = bs.match_id 
        AND b.innings_num = bs.innings_num 
        AND b.over_num = bs.over_num 
        AND b.ball_num = bs.ball_num
    GROUP BY b.striker_id, b.match_id, b.innings_num
),
dismissals_with_runs AS (
    -- Get all dismissals and corresponding runs
    SELECT 
        w.player_out_id,
        w.match_id,
        w.innings_num,
        COALESCE(mir.innings_runs, 0) as runs_at_dismissal
    FROM wickets w
    LEFT JOIN match_innings_runs mir ON w.player_out_id = mir.player_id 
        AND w.match_id = mir.match_id 
        AND w.innings_num = mir.innings_num
),
runs_data AS (
    SELECT 
    b.striker_id as player_id,
    COUNT(DISTINCT b.match_id) as matches,
    COUNT(DISTINCT CONCAT(b.match_id, '_', b.innings_num)) as innings,
    -- sum over the runs of batter_score to get total runs
    SUM(COALESCE(bs.run_scored, 0)) as total_runs,
    -- inefficient way for 100s and 50s calculations, but easier (there are problems with CTE)
    -- can make this better but efficiency ga*d maraye, assignment complete krna
    COUNT(CASE 
        WHEN (
            SELECT SUM(bs_inner.run_scored)
            FROM batter_score bs_inner
            JOIN balls b_inner ON b_inner.match_id = bs_inner.match_id 
                AND b_inner.innings_num = bs_inner.innings_num
                AND b_inner.over_num = bs_inner.over_num
                AND b_inner.ball_num = bs_inner.ball_num
            WHERE b_inner.striker_id = b.striker_id
                AND b_inner.match_id = b.match_id
                AND b_inner.innings_num = b.innings_num
        ) >= 100 THEN 1 
    END) as hundreds,
    COUNT(CASE 
        WHEN (
            SELECT SUM(bs_inner.run_scored)
            FROM batter_score bs_inner
            JOIN balls b_inner ON b_inner.match_id = bs_inner.match_id 
                AND b_inner.innings_num = bs_inner.innings_num
                AND b_inner.over_num = bs_inner.over_num
                AND b_inner.ball_num = bs_inner.ball_num
            WHERE b_inner.striker_id = b.striker_id
                AND b_inner.match_id = b.match_id
                AND b_inner.innings_num = b.innings_num
        ) BETWEEN 50 AND 99 THEN 1 
    END) as fifties,  
    --ducks, including the cases of platinum ducks
    (
            SELECT COUNT(*)
            FROM dismissals_with_runs d
            WHERE d.player_out_id = b.striker_id
            AND d.runs_at_dismissal = 0
        ) as ducks,
    -- balls faced
    COUNT(CASE 
            WHEN NOT EXISTS (
                SELECT 1 
                FROM extras e 
                WHERE e.match_id = b.match_id 
                    AND e.innings_num = b.innings_num
                    AND e.over_num = b.over_num
                    AND e.ball_num = b.ball_num
            ) THEN 1 
        END) as balls_faced,   
    -- boundaries
    SUM(CASE WHEN bs.type_run = 'boundary' THEN 1 ELSE 0 END) as boundaries,
    -- HS
    MAX((
        SELECT SUM(bs_inner.run_scored)
        FROM batter_score bs_inner
        JOIN balls b_inner ON b_inner.match_id = bs_inner.match_id 
            AND b_inner.innings_num = bs_inner.innings_num
            AND b_inner.over_num = bs_inner.over_num
            AND b_inner.ball_num = bs_inner.ball_num
        WHERE b_inner.striker_id = b.striker_id
            AND b_inner.match_id = b.match_id
            AND b_inner.innings_num = b.innings_num
    )) as highest_score    
    -- this gives the data for all the balls played by a player
    FROM balls b
    LEFT JOIN batter_score bs ON b.match_id = bs.match_id 
        AND b.innings_num = bs.innings_num 
        AND b.over_num = bs.over_num 
        AND b.ball_num = bs.ball_num
    LEFT JOIN wickets w ON b.match_id = w.match_id 
        AND b.innings_num = w.innings_num 
        AND b.over_num = w.over_num 
        AND b.ball_num = w.ball_num
        AND b.striker_id = w.player_out_id
    JOIN player_match pm ON b.striker_id = pm.player_id 
        AND b.match_id = pm.match_id
    WHERE pm.is_extra = false
    GROUP BY b.striker_id
),
not_outs AS (
    -- Calculate number of not outs
    SELECT 
        b.striker_id as player_id,
        COUNT(DISTINCT CONCAT(b.match_id, '_', b.innings_num)) - 
        COUNT(DISTINCT CASE WHEN w.player_out_id IS NOT NULL 
                           THEN CONCAT(b.match_id, '_', b.innings_num) END) as not_outs
    FROM balls b
    LEFT JOIN wickets w ON b.match_id = w.match_id 
        AND b.innings_num = w.innings_num 
        AND b.striker_id = w.player_out_id
    JOIN player_match pm ON b.striker_id = pm.player_id AND b.match_id = pm.match_id
    WHERE pm.is_extra = false
    GROUP BY b.striker_id
),
matches_played AS (
    -- Compute matches separately (includes players who never faced a ball)
    SELECT 
        pm.player_id,
        COUNT(DISTINCT pm.match_id) AS matches
    FROM player_match pm
    WHERE pm.is_extra = false
    GROUP BY pm.player_id
)
SELECT 
    r.player_id,
    COALESCE(mp.matches, 0) AS "Mat",
    r.innings as "Inns",
    r.total_runs as "R",
    r.highest_score as "HS",
    CASE 
        WHEN (r.innings - COALESCE(no.not_outs, 0)) = 0 THEN 0
        ELSE ROUND(CAST(r.total_runs AS DECIMAL) / NULLIF((r.innings - COALESCE(no.not_outs, 0)), 0), 2)
    END as "Avg",
    CASE 
        WHEN r.balls_faced = 0 THEN 0
        ELSE ROUND(CAST(r.total_runs * 100 AS DECIMAL) / NULLIF(r.balls_faced, 0), 2)
    END as "SR",
    r.hundreds as "100s",
    r.fifties as "50s",
    r.ducks as "Ducks",
    r.balls_faced as "BF",
    r.boundaries as "Boundaries",
    COALESCE(no.not_outs, 0) as "NO"
FROM runs_data r
LEFT JOIN not_outs no ON r.player_id = no.player_id
LEFT JOIN matches_played mp ON r.player_id = mp.player_id;

/* bowler_stats */
CREATE OR REPLACE VIEW bowler_stats AS
WITH bowler_deliveries AS (
    -- Get all deliveries bowled by a bowler
        SELECT 
        b.bowler_id as player_id,
        b.match_id,
        b.innings_num,
        b.over_num,
        b.ball_num,
        COALESCE(bs.run_scored, 0) as batter_runs,
        COALESCE(e.extra_runs, 0) as extra_runs,
        CASE 
            WHEN w.kind_out IN ('bowled', 'caught', 'lbw', 'stumped') THEN 1 
            ELSE 0 
        END as is_wicket
    FROM balls b
    LEFT JOIN batter_score bs ON b.match_id = bs.match_id 
        AND b.innings_num = bs.innings_num 
        AND b.over_num = bs.over_num 
        AND b.ball_num = bs.ball_num
    LEFT JOIN extras e ON b.match_id = e.match_id 
        AND b.innings_num = e.innings_num 
        AND b.over_num = e.over_num 
        AND b.ball_num = e.ball_num
    LEFT JOIN wickets w ON b.match_id = w.match_id 
        AND b.innings_num = w.innings_num 
        AND b.over_num = w.over_num 
        AND b.ball_num = w.ball_num
    JOIN player_match pm ON b.bowler_id = pm.player_id 
        AND b.match_id = pm.match_id
    WHERE pm.is_extra = false
),
bowler_aggregates AS (
    -- Calculate aggregated statistics per bowler
    SELECT 
        player_id,
        COUNT(*) as balls_bowled,
        SUM(batter_runs + extra_runs) as total_runs,
        SUM(is_wicket) as wickets,
        SUM(extra_runs) as extras,
        COUNT(DISTINCT CONCAT(match_id, '_', innings_num, '_', over_num)) as overs_bowled
    FROM bowler_deliveries
    GROUP BY player_id
)
SELECT 
    ba.player_id,
    ba.balls_bowled as "B",
    ba.wickets as "W",
    ba.total_runs as "Runs",
    CASE 
        WHEN ba.wickets = 0 THEN 0
        ELSE ROUND(CAST(ba.total_runs AS DECIMAL) / ba.wickets, 2)
    END as "Avg",
    CASE 
        WHEN ba.overs_bowled = 0 THEN 0
        ELSE ROUND(CAST(ba.total_runs AS DECIMAL) / ba.overs_bowled, 2)
    END as "Econ",
    CASE 
        WHEN ba.wickets = 0 THEN 0
        ELSE ROUND(CAST(ba.balls_bowled AS DECIMAL) / ba.wickets, 2)
    END as "SR",
    ba.extras as "Extras"
FROM bowler_aggregates ba;

/* fielder stats */
CREATE OR REPLACE VIEW fielder_stats AS
WITH fielding_actions AS (
    -- Get all fielding actions for each player
    SELECT 
        w.fielder_id as player_id,
        -- Use COALESCE to convert NULL to 0 for each type of dismissal
        COALESCE(SUM(CASE 
            WHEN w.kind_out = 'caught' THEN 1 
            ELSE 0 
        END), 0) as catches,
        COALESCE(SUM(CASE 
            WHEN w.kind_out = 'stumped' THEN 1 
            ELSE 0 
        END), 0) as stumpings,
        COALESCE(SUM(CASE 
            WHEN w.kind_out = 'runout' THEN 1 
            ELSE 0 
        END), 0) as runouts
    FROM wickets w
    WHERE w.fielder_id IS NOT NULL
    GROUP BY w.fielder_id
)
SELECT 
    f.player_id,
    f.catches as "C",
    f.stumpings as "St",
    f.runouts as "RO"
FROM fielding_actions f;

--TESTING =======================================================================