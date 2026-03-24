use startersql;

select * from users;

select id , 
name ,
concat( id , name ) as id_name
from users;


insert into users 
values
('Rahul','rahul@gmail.com','male','1994-03-15','2026-02-10 18:01:12.120',1),
('Alicia','alicia@gmail.com','female','1995-01-02','2026-02-10 18:02:56.103',1),
('Aman','aman@gmail.com','male','1993-07-21','2026-02-10 18:05:44.567',1),
('Neha','neha@gmail.com','female','1996-11-30','2026-02-10 18:07:33.890',1),
('Kiran','kiran@gmail.com','male','1992-05-18','2026-02-10 18:10:11.345',1),
('Pooja','pooja@gmail.com','female','1997-09-09','2026-02-10 18:12:25.678',1),
('Vikram','vikram@gmail.com','male','1991-12-25','2026-02-10 18:15:49.234',1);


sp_help users;
exec sp_columns 'users
exec sp_spaceused 'users' 
exec sp_tables;

