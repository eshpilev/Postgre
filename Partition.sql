CREATE OR REPLACE PROCEDURE public.add_partitions(table_name character varying, end_date timestamp without time zone, duration_interval interval, INOUT created_partitions character varying[])
 LANGUAGE plpgsql
 SET search_path TO 'public'
AS $procedure$
	DECLARE			
		partition_name varchar(512);
		interval_name varchar(32); 
		start_date_part timestamp;
		end_date_part timestamp;	
		query text; 
	BEGIN				
		start_date_part = get_partition_last_date(table_name) + 1;
		WHILE start_date_part < end_date 
		LOOP   			
			end_date_part = start_date_part + duration_interval - interval '1 microsecond';	
			interval_name = get_interval_name(start_date_part, duration_interval);	
			partition_name = format('%s_%s', table_name, interval_name);
			query = 
				format(E'CREATE TABLE %I (LIKE %I INCLUDING DEFAULTS);\n',
						partition_name, table_name) ||
				format(E'ALTER TABLE %I ADD CONSTRAINT check_%s CHECK (actual_date >= %L AND actual_date <= %L);\n',
						partition_name, partition_name, start_date_part, end_date_part) ||	
				format(E'WITH moved_rows AS (
							DELETE FROM %I
							WHERE actual_date >= %L AND actual_date <= %L
							RETURNING * 
						)
						INSERT INTO %I
						SELECT * FROM moved_rows;\n',
						format('%s_default', table_name),  start_date_part, end_date_part, partition_name) ||				
				format(E'ALTER TABLE %I ATTACH PARTITION %I FOR VALUES FROM (%L) TO (%L);\n',
						table_name, partition_name, start_date_part, end_date_part) ||						
				format(E'ALTER TABLE %I DROP CONSTRAINT check_%s;',
						partition_name, partition_name); 						
			RAISE NOTICE E'Добавление партиции %:\n%', partition_name, query; 		
			EXECUTE query;				
			created_partitions = created_partitions || partition_name;
			start_date_part = start_date_part + duration_interval;		
		END LOOP;   			
	END;
$procedure$
;


CREATE OR REPLACE PROCEDURE public.create_partitions(IN table_name character varying, IN start_date timestamp without time zone, IN end_date timestamp without time zone, IN duration_interval interval)
 LANGUAGE plpgsql
 SET search_path TO 'public'
AS $procedure$
	DECLARE						
		partition_name varchar(512);
		interval_name varchar(32); 
		start_date_part timestamp = start_date;
		end_date_part timestamp;
		query text; 
	BEGIN  			
		IF NOT EXISTS (SELECT * FROM pg_class AS c
					JOIN pg_namespace AS ns
					ON c.relnamespace = ns.oid
					WHERE c.relname = table_name AND ns.nspname = CURRENT_SCHEMA) THEN    	
			RAISE EXCEPTION 'Таблица % не существует', partition_name;
		END IF;  			
			 
		WHILE start_date_part < end_date 
		LOOP   		
			end_date_part = start_date_part + duration_interval - interval '1 microsecond';			
			interval_name = get_interval_name(start_date_part, duration_interval);	    	
			partition_name = format('%s_%s', table_name, interval_name);
			query = format('CREATE TABLE %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
				partition_name, table_name, start_date_part, end_date_part);  
			RAISE NOTICE 'Создание партиции за интервал времени: %', query;  
			EXECUTE query;	
			start_date_part = start_date_part + duration_interval;				
		END LOOP;   			

		query = format('CREATE TABLE %I PARTITION OF %I DEFAULT',
				format('%s_default', table_name), table_name);  
		RAISE NOTICE 'Создание default партиции за интервал времени: %', query;  
		EXECUTE query;		  
	END;
$procedure$
;


CREATE OR REPLACE PROCEDURE public.drop_partition_lossless(partition_name character varying)
 LANGUAGE plpgsql
 SET search_path TO 'public'
AS $procedure$
DECLARE		
	partition_oid oid;
	parent_name varchar(512);
	default_name varchar(512);
	query text; 
BEGIN  	
	partition_oid = (SELECT c."oid" FROM pg_class AS c
		JOIN pg_namespace AS ns
		ON c.relnamespace = ns.oid
		WHERE c.relname = partition_name AND ns.nspname = CURRENT_SCHEMA
		LIMIT 1);   

	IF partition_oid IS NULL 
		THEN RAISE EXCEPTION 'Партиция % не существует', partition_name;
	END IF;

	parent_name = (SELECT base_tb.relname
		FROM pg_class base_tb 
		JOIN pg_inherits i on i.inhparent = base_tb.oid 
		JOIN pg_class pt on pt.oid = i.inhrelid 		
		WHERE pt.oid = partition_oid			
		LIMIT 1);   

	IF parent_name IS NULL 
		THEN RAISE EXCEPTION 'Для партиции % не найдена главная таблица', partition_name;
	END IF;

	default_name = format('%s_default', parent_name);

	IF NOT EXISTS (	SELECT * FROM pg_class AS c
					JOIN pg_namespace AS ns
					ON c.relnamespace = ns.oid
					WHERE c.relname = default_name AND ns.nspname = CURRENT_SCHEMA) 
	THEN    		
		RAISE EXCEPTION 'Не найдена партиция по умолчанию %', default_name;
	END IF;    		

	query = 
		format(E'ALTER TABLE %I DETACH PARTITION %I;\n',
				parent_name, partition_name) ||			
		format(E'WITH moved_rows AS (
				    DELETE FROM %I				   
				    RETURNING * 
				)
				INSERT INTO %I
				SELECT * FROM moved_rows;\n',
				partition_name, default_name) ||						
		format(E'DROP TABLE %I', partition_name); 						

	RAISE NOTICE E'Удаление партиции %:\n%', partition_name, query;  
	EXECUTE query;					
END;
$procedure$
;


CREATE OR REPLACE FUNCTION public.get_partition_last_date(source_name character varying)
 RETURNS date
 LANGUAGE plpgsql
 SET search_path TO 'public'
AS $function$
	DECLARE 
		split_expression text[];
		last_date text;
	BEGIN		
		split_expression = (SELECT string_to_array(pg_get_expr(pt.relpartbound, pt.oid, true), '(''')
			FROM pg_class base_tb 
			JOIN pg_inherits i ON i.inhparent = base_tb.oid 
			JOIN pg_class pt ON pt.oid = i.inhrelid
			WHERE base_tb.oid = source_name::regclass
			AND pg_get_expr(pt.relpartbound, pt.oid, true) <> 'DEFAULT'
			ORDER BY pt.relname DESC		
			LIMIT 1);   		

		last_date = (string_to_array(split_expression[array_length(split_expression, 1)], ' '))[1];			
		RETURN last_date::date;
	END
$function$
;

CREATE OR REPLACE FUNCTION public.get_interval_name(start_date timestamp without time zone, duration_interval interval)
 RETURNS character varying
 LANGUAGE plpgsql
AS $function$
	DECLARE 
		format  varchar(8);
		start_name varchar(16);
		end_name varchar(16);
		end_date timestamp = start_date + duration_interval - interval '1 microsecond';
	BEGIN   
		IF (duration_interval < '1 day') THEN
			RAISE EXCEPTION 'Ошибка при определении постфикса для интервала %. Интервал не может быть меньше одного дня', duration_interval;  
		END IF;

		CASE 
			WHEN lower(duration_interval::varchar) LIKE '%day%' THEN    		
				format = 'YYYYMMDD';					
			WHEN lower(duration_interval::varchar) LIKE '%mon%' THEN  
				IF date_part('day', start_date) = 1 THEN format = 'YYYYMM';				
				ELSE format = 'YYYYMMDD';					
				END IF; 
			WHEN lower(duration_interval::varchar) LIKE '%year%' THEN   
				IF (date_part('day', start_date) = 1 AND date_part('mon', start_date) = 1) THEN format = 'YYYY';	 				
				ELSIF date_part('day', start_date) = 1 THEN format = 'YYYYMM'; 
				ELSE format = 'YYYYMMDD';
				END IF; 
			ELSE 
				RAISE EXCEPTION 'Не удалось сформировать постфикс для интервала %', duration_interval;   			
		END CASE; 

		start_name = to_char(start_date, format);
		end_name = to_char(end_date, format);
		IF start_name = end_name THEN RETURN start_name;
		ELSE RETURN format('%s_%s', start_name, end_name); 	
		END IF; 
	END;
$function$
;