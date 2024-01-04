do $$
begin
for r in 1..10 loop
insert into category(category_id, name) values(r, 'Category' || r);
end loop;
end;
$$;