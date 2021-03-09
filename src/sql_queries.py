import configparser


###### CONFIG
config = configparser.ConfigParser()
config.read('./aws/credentials.cfg')

# role to connect to redshigt (created previously)
ROLE_ARN = config.get("IAM_ROLE","ARN")
# gets S3 uri for each data set
AIRLINES_DATA = config.get("S3","AIRLINES_DATA")
COUNTRIES_DATA = config.get("S3","COUNTRIES_DATA")
DEMO_DATA = config.get("S3","DEMO_DATA")
I94_DATA = config.get("S3","I94_DATA")
MODE_DATA = config.get("S3","MODE_DATA")
PORT_DATA = config.get("S3","PORT_DATA")
STATES_DATA = config.get("S3","STATES_DATA")
VISAS_DATA = config.get("S3","VISAS_DATA")

###### DROP TABLES

# staging tables
staging_i94_table_drop = "DROP TABLE IF EXISTS staging_i94"
staging_demo_table_drop = "DROP TABLE IF EXISTS staging_demographics"
staging_states_table_drop = "DROP TABLE IF EXISTS staging_states"

# fact and dimension tables
student_arrivals_table_drop = "DROP TABLE IF EXISTS student_arrivals_fact"
country_table_drop = "DROP TABLE IF EXISTS dim_country"
entry_mode_table_drop = "DROP TABLE IF EXISTS dim_entry_mode"
visatype_table_drop = "DROP TABLE IF EXISTS dim_visa_type"
port_of_entry_table_drop = "DROP TABLE IF EXISTS dim_entry_port"
airline_table_drop = "DROP TABLE IF EXISTS dim_airline"
date_table_drop = "DROP TABLE IF EXISTS dim_date"
destination_state_table_drop = "DROP TABLE IF EXISTS dim_destination_state"

###### TRUNCATE TABLES

# staging tables
staging_i94_table_trun = "TRUNCATE TABLE staging_i94"
staging_demo_table_trun = "TRUNCATE TABLE staging_demographics"
staging_states_table_trun = "TRUNCATE TABLE staging_states"

# dimension tables only(except dim_date)
country_table_trun = "TRUNCATE TABLE dim_country"
entry_mode_table_trun = "TRUNCATE TABLE dim_entry_mode"
visatype_table_trun = "TRUNCATE TABLE dim_visa_type"
port_of_entry_table_trun = "TRUNCATE TABLE dim_entry_port"
airline_table_trun = "TRUNCATE TABLE dim_airline"
destination_state_table_trun = "TRUNCATE TABLE dim_destination_state"


###### CREATE TABLES

# staging tables
staging_i94_table_create = ("""CREATE TABLE IF NOT EXISTS staging_i94 (
                                   cicid INT8,
                                   i94cit INT,
                                   i94res INT,
                                   i94port VARCHAR,
                                   arrdate DATE,
                                   i94mode INT,
                                   i94addr VARCHAR,
                                   depdate DATE,
                                   i94bir INT,
                                   entdepa VARCHAR,
                                   entdepd VARCHAR,
                                   entdepu VARCHAR,
                                   matflag VARCHAR,
                                   biryear INT,
                                   dtaddto VARCHAR,
                                   gender VARCHAR,
                                   airline VARCHAR,
                                   fltno VARCHAR,
                                   visatype VARCHAR                                 
                               )""")

staging_demo_table_create = ("""CREATE TABLE IF NOT EXISTS staging_demographics (
                                    state_code VARCHAR,
                                    median_age FLOAT,
                                    male_population FLOAT,
                                    female_population FLOAT,
                                    total_population INT8,
                                    number_of_veterans FLOAT,
                                    foreign_born FLOAT,
                                    average_household_size FLOAT,
                                    american_indian_and_alaska_native FLOAT,
                                    black_or_african_american FLOAT,
                                    hispanic_or_latino FLOAT,
                                    white FLOAT,
                                    asian FLOAT
                                )""")

staging_states_table_create = ("""CREATE TABLE IF NOT EXISTS staging_states (code VARCHAR, description VARCHAR)""")

# fact and dimension tables
student_arrivals_table_create = ("""CREATE TABLE IF NOT EXISTS student_arrivals_fact (
                                        id_cic INT8 PRIMARY KEY, 
                                        id_citizenship INT,
                                        id_residency INT,
                                        id_visatype VARCHAR NOT NULL,
                                        id_port VARCHAR NOT NULL,
                                        id_mode INT,
                                        id_state VARCHAR,
                                        id_airline VARCHAR,
                                        arrival_date DATE NOT NULL sortkey,
                                        flight_num VARCHAR,
                                        departure_date DATE,
                                        admitted_date VARCHAR,
                                        arrival_flag VARCHAR,
                                        departure_flag VARCHAR,
                                        update_flag VARCHAR,
                                        match_flag VARCHAR,
                                        std_age INT,
                                        std_biryear INT,
                                        std_gender VARCHAR,
                                        CONSTRAINT fk_country_cit
                                            FOREIGN KEY(id_citizenship) 
                                                REFERENCES dim_country(id_country),
                                        CONSTRAINT fk_country_res
                                            FOREIGN KEY(id_residency) 
                                                REFERENCES dim_country(id_country),
                                        CONSTRAINT fk_visatype
                                            FOREIGN KEY(id_visatype) 
                                                REFERENCES dim_visa_type(id_visatype),
                                        CONSTRAINT fk_port
                                            FOREIGN KEY(id_port) 
                                                REFERENCES dim_entry_port(id_port),
                                        CONSTRAINT fk_mode
                                            FOREIGN KEY(id_mode) 
                                                REFERENCES dim_entry_mode(id_mode),
                                        CONSTRAINT fk_state
                                            FOREIGN KEY(id_state) 
                                                REFERENCES dim_destination_state(id_state),
                                        CONSTRAINT fk_airline
                                            FOREIGN KEY(id_airline) 
                                                REFERENCES dim_airline(id_airline),
                                        CONSTRAINT fk_date
                                            FOREIGN KEY(arrival_date) 
                                                REFERENCES dim_date(arrival_date)
                                    )""")

country_table_create = ("""CREATE TABLE IF NOT EXISTS dim_country (id_country INT PRIMARY KEY, 
                                                                   description VARCHAR) 
                                                                   diststyle all""")

entry_mode_table_create = ("""CREATE TABLE IF NOT EXISTS dim_entry_mode (id_mode INT PRIMARY KEY, 
                                                                         description VARCHAR)
                                                                         diststyle all""")

visatype_table_create = ("""CREATE TABLE IF NOT EXISTS dim_visa_type (id_visatype VARCHAR PRIMARY KEY, 
                                                                      description VARCHAR)
                                                                      diststyle all""")

port_of_entry_table_create = ("""CREATE TABLE IF NOT EXISTS dim_entry_port (id_port VARCHAR PRIMARY KEY, 
                                                                            description VARCHAR)
                                                                            diststyle all""")

airline_table_create = ("""CREATE TABLE IF NOT EXISTS dim_airline (id_airline VARCHAR PRIMARY KEY, 
                                                                   description VARCHAR)
                                                                   diststyle all""")

date_table_create = ("""CREATE TABLE IF NOT EXISTS dim_date (
                            arrival_date DATE PRIMARY KEY sortkey,
                            day INT, 
                            week INT, 
                            month INT,
                            year INT, 
                            weekday VARCHAR
                        ) diststyle all""")

destination_state_table_create = ("""CREATE TABLE IF NOT EXISTS dim_destination_state (
                                         id_state VARCHAR PRIMARY KEY,
                                         state_name VARCHAR,
                                         median_age FLOAT,
                                         male_pop INT,
                                         female_pop INT,
                                         veterans INT,
                                         foreign_born INT,
                                         avg_hh_size FLOAT,
                                         american_native INT,
                                         african_american INT,
                                         hispanic_latino INT,
                                         white INT,
                                         asian INT
                                     ) diststyle all""")


###### STAGING TABLES COPY QUERIES

staging_i94_copy = ("""COPY staging_i94 
                       FROM '{}'
                       IAM_ROLE '{}'
                       FORMAT AS PARQUET;
                    """).format(I94_DATA, ROLE_ARN)

staging_demo_copy = ("""COPY staging_demographics
                        FROM '{}'
                        IAM_ROLE '{}'
                        FORMAT AS PARQUET;
                     """).format(DEMO_DATA, ROLE_ARN)

staging_states_copy = ("""COPY staging_states
                          FROM '{}'
                          IAM_ROLE '{}'
                          FORMAT AS PARQUET;
                       """).format(STATES_DATA, ROLE_ARN)

###### FINAL TABLES INSERTS QUERIES

student_arrivals_table_insert = ("""INSERT INTO student_arrivals_fact (id_cic, id_citizenship, id_residency, id_visatype,
                                                                       id_port, id_mode, id_state, id_airline, arrival_date,
                                                                       flight_num, departure_date, admitted_date,
                                                                       arrival_flag, departure_flag, update_flag,
                                                                       match_flag, std_age, std_biryear, std_gender)
                                    SELECT cicid, i94cit, i94res, visatype,
                                           i94port, i94mode, i94addr, airline, arrdate,
                                           fltno, depdate, dtaddto,
                                           entdepa, entdepd, entdepu,
                                           matflag, i94bir, biryear, gender
                                    FROM staging_i94
                                 """)

country_table_insert = ("""COPY dim_country
                           FROM '{}'
                           IAM_ROLE '{}'
                           FORMAT AS PARQUET;
                        """).format(COUNTRIES_DATA, ROLE_ARN)

entry_mode_table_insert = ("""COPY dim_entry_mode
                              FROM '{}'
                              IAM_ROLE '{}'
                              FORMAT AS PARQUET;
                           """).format(MODE_DATA, ROLE_ARN)

visatype_table_insert = ("""COPY dim_visa_type
                            FROM '{}'
                            IAM_ROLE '{}'
                            FORMAT AS PARQUET;
                         """).format(VISAS_DATA, ROLE_ARN)

port_of_entry_table_insert = ("""COPY dim_entry_port
                                 FROM '{}'
                                 IAM_ROLE '{}'
                                 FORMAT AS PARQUET;
                              """).format(PORT_DATA, ROLE_ARN)

airline_table_insert = ("""COPY dim_airline
                           FROM '{}'
                           IAM_ROLE '{}'
                           FORMAT AS PARQUET;
                        """).format(AIRLINES_DATA, ROLE_ARN)
                         
date_table_insert = ("""INSERT INTO dim_date (arrival_date, day, week, month, year, weekday)
                        SELECT DISTINCT
                               arrdate,
                               EXTRACT(DAY FROM arrdate) AS day,
                               EXTRACT(WEEK FROM arrdate) AS week,
                               EXTRACT(MONTH FROM arrdate) AS month,
                               EXTRACT(YEAR FROM arrdate) AS year,
                               EXTRACT(DOW FROM arrdate) AS weekday
                        FROM staging_i94
                     """)

destination_state_table_insert = ("""INSERT INTO dim_destination_state (id_state, state_name, median_age, male_pop,
                                                                        female_pop, veterans, foreign_born, avg_hh_size,
                                                                        american_native, african_american, hispanic_latino, 
                                                                        white, asian)
                                     SELECT s.code, s.description, d.median_age, d.male_population,
                                            d.female_population, d.number_of_veterans, d.foreign_born, 
                                            d.average_household_size, d.american_indian_and_alaska_native, 
                                            d.black_or_african_american, d.hispanic_or_latino, d.white, d.asian
                                     FROM staging_states s
                                          LEFT JOIN staging_demographics d on (s.code = d.state_code)
                                  """)


##### QUERY LISTS

drop_table_queries = [student_arrivals_table_drop, country_table_drop, entry_mode_table_drop, visatype_table_drop,
                     port_of_entry_table_drop, airline_table_drop, date_table_drop, destination_state_table_drop,
                     staging_i94_table_drop, staging_demo_table_drop, staging_states_table_drop]

truncate_table_queries = [country_table_trun, entry_mode_table_trun, visatype_table_trun,
                          port_of_entry_table_trun, airline_table_trun, destination_state_table_trun,
                          staging_i94_table_trun, staging_demo_table_trun, staging_states_table_trun]

create_table_queries = [country_table_create, entry_mode_table_create, visatype_table_create, 
                        port_of_entry_table_create, airline_table_create, date_table_create, 
                        destination_state_table_create, student_arrivals_table_create,
                        staging_i94_table_create, staging_demo_table_create, staging_states_table_create]

copy_table_queries = [staging_i94_copy, staging_demo_copy, staging_states_copy]

insert_table_queries = [country_table_insert, entry_mode_table_insert, visatype_table_insert, 
                        port_of_entry_table_insert, airline_table_insert, date_table_insert,
                        destination_state_table_insert, student_arrivals_table_insert]