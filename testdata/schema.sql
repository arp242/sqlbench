drop table if exists cpu_usage;
create table cpu_usage (
	ts     timestamptz,
	host   text,
	usage  double precision
);
