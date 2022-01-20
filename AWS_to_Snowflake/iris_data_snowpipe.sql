create or replace database etl_db;
use etl_db;

create or replace table etl_db.PUBLIC.Iris_dataset(
  sepallength number(10,5),
  sepalwidth number(10,5),
  petallength number(10,5),
  petalwidth number(10,5),
  class varchar(30)
);

create or replace file format iris_file
  type = csv
  field_delimiter = ','
  skip_header = 1
  field_optionally_enclosed_by = '"'
  null_if = ('NULL', 'null')
  empty_field_as_null = true;
  
create or replace stage etl_db.PUBLIC.iris_stage
url = "s3://destinationfile2001"
credentials=(aws_key_id='xxx' aws_secret_key = 'XXX')
file_format = iris_file

list@iris_stage


create or replace pipe iris_pipe
auto_ingest = true
as
    copy into etl_db.PUBLIC.Iris_dataset
    from @etl_db.PUBLIC.iris_stage
    file_format = (format_name  = iris_file);
    

show pipes

desc pipe iris_pipe

select * from Iris_dataset 

--Checking status of pipe
select SYSTEM$PIPE_STATUS('iris_pipe')

select *
  from table(information_schema.pipe_usage_history(
    date_range_start=>dateadd('hour',-12,current_timestamp()),
    pipe_name=>'iris_pipe'))

alter pipe iris_pipe set pipe_execution_paused = true;

