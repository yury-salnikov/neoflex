-- Скрипт создания процедур логирования

create or replace procedure logs.Print(level text, message text)
language plpgsql
as $$
begin
	insert into logs.events(level, message) values(level, message);
end;$$;


create or replace procedure logs.PrintError(message text)
language plpgsql
as $$
begin
	call logs.Print('error', message);
end;$$;


create or replace procedure logs.PrintWarning(message text)
language plpgsql
as $$
begin
	call logs.Print('warning', message);
end;$$;


create or replace procedure logs.PrintInfo(message text)
language plpgsql
as $$
begin
	call logs.Print('info', message);
end;$$;