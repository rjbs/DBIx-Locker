CREATE TABLE locks (
  id INTEGER PRIMARY KEY,
  lockstring varchar(128) UNIQUE,
  created datetime NOT NULL,
  expires datetime NOT NULL,
  locked_by varchar(1024)
);
