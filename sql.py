# DROP TABLES
dim_users_drop = "DROP TABLE IF EXISTS dim_users"
users_friends_drop = "DROP TABLE IF EXISTS users_friends"
users_elite_drop = "DROP TABLE IF EXISTS users_elite"
dim_business_drop = "DROP TABLE IF EXISTS dim_business"
business_checkin_drop = "DROP TABLE IF EXISTS business_checkin"
fact_tip_drop = "DROP TABLE IF EXISTS fact_tip"
fact_review_drop = "DROP TABLE IF EXISTS fact_review"

#-------------------------------------------dim_users-----------------------------------------------#
dim_users_create = ("""
                   CREATE TABLE IF NOT EXISTS dim_users (
                       	user_id varchar(300),
                        name varchar(300),
                        review_count integer,
                        yelping_since varchar,
                        useful integer,
                        funny integer,
                        cool integer,
                        elite varchar,
                        friends varchar,
                        fans integer,
                        average_stars decimal,
                        compliment_hot integer,
                        compliment_more integer,
                        compliment_profile integer,
                        compliment_cute integer,
                        compliment_list integer,
                        compliment_note integer,
                        compliment_plain integer,
                        compliment_cool integer,
                        compliment_funny integer,
                        compliment_writer integer,
                        compliment_photos integer,
                        PRIMARY KEY (user_id)
                   )
                    """)

users_friends_create = ("""
                   CREATE TABLE IF NOT EXISTS users_friends (
                        user_id VARCHAR(300),
                        friend_id VARCHAR(300),
                        FOREIGN KEY (user_id) REFERENCES dim_users(user_id)
                   )
                    """)

users_elite_create = ("""
				   CREATE TABLE IF NOT EXISTS users_elite (
                        user_id VARCHAR(300),
                        elite VARCHAR(300),
                        FOREIGN KEY (user_id) REFERENCES dim_users(user_id)
                   )
                    """)

dim_users_insert = ("""
                        INSERT INTO dim_users
                        SELECT 
                        *
                        FROM users
                        where user_id IS NOT NULL
                        """)

users_friends_insert = ("""
                    insert into users_friends
                    select a as user_id,trim(unnest(b)) as friend_id
                    from (
                        select user_id,regexp_split_to_array(friends, ',') from users
                    ) as dt(a,b)
                    """)

users_elite_insert = ("""
                    insert into users_elite
                    select a as user_id,trim(unnest(b)) as elite
                    from (
                        select user_id,regexp_split_to_array(elite, ',') from users
                    ) as dt(a,b)
                    """)

#------------------------------------------------------------------------------------------#
#-------------------------------------------dim_business-----------------------------------------------#

dim_business_create = ("""
                   CREATE TABLE IF NOT EXISTS dim_business (
                    business_id VARCHAR(300),
                    name VARCHAR(300),
                    address VARCHAR(300),
                    city VARCHAR(300),
                    state VARCHAR(300),
                    postal_code VARCHAR(300),
                    latitude DECIMAL,
                    longitude DECIMAL,
                    stars DECIMAL,
                    review_count integer,
                    is_open integer,
                    attributes json,
                    categories VARCHAR,
                    hours json,
                    PRIMARY KEY (business_id)
                   )
                    """)

dim_business_insert = ("""
                        INSERT INTO dim_business
                        SELECT 
                        *
                        FROM business
                        where business_id is not null
                        """)

business_checkin_create = ("""
                   CREATE TABLE IF NOT EXISTS business_checkin (
                        business_id VARCHAR(300),
                        date timestamp,
                        FOREIGN KEY (business_id) REFERENCES dim_business(business_id)
                   )
                    """)

business_checkin_insert = ("""
                    insert into business_checkin
                        select a as business_id,TO_TIMESTAMP((unnest(b)),'YYYY-MM-DD HH24:MI:SS') as date
                        from (
                            select business_id,regexp_split_to_array(date, ',')
                            from checkin 
                        ) as dt(a,b)
                    """)

#------------------------------------------------------------------------------------------#
#-------------------------------------------fact_tip-----------------------------------------------#

fact_tip_create = ("""
            CREATE TABLE IF NOT EXISTS fact_tip
                (
                    user_id varchar(300),
                    business_id varchar(300),
                    text varchar,
                    date timestamp,
                    compliment_count integer,
                    FOREIGN KEY (user_id) REFERENCES dim_users(user_id),
                    FOREIGN KEY (business_id) REFERENCES dim_business(business_id)
                )
        """)

fact_tip_insert = ("""
                    insert into fact_tip 
                    select 
                    a.user_id as user_id,
                    a.business_id as business_id,
                    a.text as text,
                    TO_TIMESTAMP(a.date,'YYYY-MM-DD HH24:MI:SS') as date,
                    compliment_count 
                    from tip a
                    inner join dim_business b
                    on a.business_id = b.business_id
                    inner join dim_users c
                    on a.user_id = c.user_id
        """)
#------------------------------------------------------------------------------------------#
#-------------------------------------------fact_review-----------------------------------------------#
fact_review_create = ("""
                CREATE TABLE IF NOT EXISTS fact_review
                (
                review_id varchar(300),
                user_id varchar(300),
                business_id varchar(300),
                stars decimal,
                text varchar,
                date timestamp,
                FOREIGN KEY (user_id) REFERENCES dim_users(user_id),
                FOREIGN KEY (business_id) REFERENCES dim_business(business_id)
                )
            """)

fact_review_insert = ("""
                    INSERT INTO fact_review
                    SELECT 
                    review_id,
                    a.user_id as user_id,
                    a.business_id as business_id,
                    a.stars as stars,
                    a.text as text,
                    TO_TIMESTAMP(date,'YYYY-MM-DD HH24:MI:SS') as date
                    FROM review a
                    inner join dim_business b
                    on a.business_id = b.business_id
                    inner join dim_users c
                    on a.user_id = c.user_id
            """)
#------------------------------------------------------------------------------------------#



# QUERY LISTS

drop_table_queries = [ users_friends_drop, users_elite_drop,dim_users_drop, business_checkin_drop, dim_business_drop, fact_tip_drop,fact_review_drop]

create_table_queries = [ dim_users_create, users_friends_create, users_elite_create, dim_business_create, business_checkin_create, fact_tip_create, fact_review_create]

insert_Dim_queries = [ dim_users_insert, users_friends_insert, users_elite_insert, dim_business_insert, business_checkin_insert]

insert_fact_queries = [ fact_tip_insert, fact_review_insert  ]


