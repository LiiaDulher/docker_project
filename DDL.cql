CREATE  KEYSPACE wikipedia_dulher WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1 };

USE wikipedia_dulher;

CREATE TABLE page_creation (uid UUID, time_created timestamp, domain text, user_id text, user_name text, is_bot boolean, page_title text, PRIMARY KEY (time_created, domain, uid));

CREATE TABLE domain_statistics (uid UUID, time_start timestamp, domain text, all_pages int, PRIMARY KEY (time_start, uid));

CREATE TABLE bots_statistics (uid UUID, time_start timestamp, domain text, created_by_bot int, PRIMARY KEY (time_start, uid));

CREATE TABLE users_statistics (uid UUID, time_start timestamp, user_id text, user_name text, page_count int, pages_list list<text>, PRIMARY KEY (time_start, uid));

CREATE TABLE pages (page_id text, page_url text, PRIMARY KEY (page_id));

CREATE TABLE users_pages (uid UUID, user_id text, page_url text, PRIMARY KEY(user_id, uid));
