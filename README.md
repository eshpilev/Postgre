# Postgre
Скрипты для PostgreSQL:

1. Partition.sql - скрипт для автоматического создания партиций с ранжированием по дате. Пример использования:
```   
CREATE TABLE public.operations (
	id int4 NOT NULL GENERATED BY DEFAULT AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE),
	description varchar(360) NOT NULL,
	actual_date timestamp NOT NULL
)
PARTITION BY RANGE (actual_date);
CREATE INDEX ON public.operations (actual_date);

CALL public.create_partitions('operations', '20200101', '20241231', '1 year');

INSERT INTO public.operations (description, actual_date)
VALUES ('OP1', '20240101'), ('OP2', '20300101');

CALL public.add_partitions('operations', '20301231', '1 year', array[]::varchar[]);
CALL public.drop_partition_lossless('operations_2030');
```
