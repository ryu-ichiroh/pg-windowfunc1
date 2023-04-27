-- drop table book_authors, authors, books;

-- 大元の課題: many to many をバッチで取得したい, 順序も考慮したい

-- 今詰まっているところ: window関数で order by が使えない

-- many to many の1例として以下のスキーマを考える
-- 本と著者のテーブル
-- 共著を考慮して、1つの本に対して複数の著者がいる
-- 一人の著者が複数の本を出せる

-- books >- book_authors -< authors

-- 大元の課題を解決するには、複数のbook_idからauthorsを順序を考慮して取得できればok

-- テーブル作成
create table "books" (
  book_id uuid primary key,
  title text not null
);

create table "authors" (
  author_id uuid primary key,
  name text not null
);

create table "book_authors" (
  book_id uuid not null references books(book_id),
  author_id uuid not null references authors(author_id),
  position integer not null,
  primary key (book_id, author_id)
);
create index on book_authors(author_id);

-- データ投入
insert into "books" (book_id, title) values 
  ('1FB112D1-54C9-4308-99C6-0163BFD0172D', '本1'),
  ('554BC347-F36C-4766-B66F-D651C84C56BA', '本2');

insert into "authors" (author_id, name) values 
  ('BED827FF-6847-41BD-88A0-B77FDD74BEA3', '著者1'),
  ('467021FD-AE39-4BA0-BC7B-3E5B21EF69F9', '著者2'),
  ('F66C5C85-5044-4433-B631-C01C64A7A4F6', '著者3');

insert into "book_authors" (book_id, author_id, position) values 
  ('1FB112D1-54C9-4308-99C6-0163BFD0172D', 'BED827FF-6847-41BD-88A0-B77FDD74BEA3', 1), -- 本1 著者1
  ('1FB112D1-54C9-4308-99C6-0163BFD0172D', '467021FD-AE39-4BA0-BC7B-3E5B21EF69F9', 2), -- 本1 著者2
  ('554BC347-F36C-4766-B66F-D651C84C56BA', 'BED827FF-6847-41BD-88A0-B77FDD74BEA3', 1), -- 本2 著者1
  ('554BC347-F36C-4766-B66F-D651C84C56BA', 'F66C5C85-5044-4433-B631-C01C64A7A4F6', 2); -- 本2 著者3


-- booksをバッチ取得
-- many to many の1回目のリクエストを想定しているが、今回の課題のポイントではない
select
  books.*
from 
  books
where
  books.book_id in ('1FB112D1-54C9-4308-99C6-0163BFD0172D', '554BC347-F36C-4766-B66F-D651C84C56BA');
-- 結果
--                book_id                | title
-- --------------------------------------+-------
--  1fb112d1-54c9-4308-99c6-0163bfd0172d | 本1
--  554bc347-f36c-4766-b66f-d651c84c56ba | 本2
-- (2 rows)



-- authorsをバッチ取得 - パターン1
-- すごくシンプルだが、authorsの情報が被ってしまう
select
  book_authors.book_id,
  authors.*,
  ROW_NUMBER() over (partition by book_authors.book_id order by book_authors."position") as "position"
from 
  authors
join book_authors using (author_id)
where book_authors.book_id in ('1FB112D1-54C9-4308-99C6-0163BFD0172D', '554BC347-F36C-4766-B66F-D651C84C56BA');

-- 結果
--                book_id                |              author_id               | name  | position
-- --------------------------------------+--------------------------------------+-------+----------
--  1fb112d1-54c9-4308-99c6-0163bfd0172d | bed827ff-6847-41bd-88a0-b77fdd74bea3 | 著者1 |        1
--  1fb112d1-54c9-4308-99c6-0163bfd0172d | 467021fd-ae39-4ba0-bc7b-3e5b21ef69f9 | 著者2 |        2
--  554bc347-f36c-4766-b66f-d651c84c56ba | bed827ff-6847-41bd-88a0-b77fdd74bea3 | 著者1 |        1
--  554bc347-f36c-4766-b66f-d651c84c56ba | f66c5c85-5044-4433-b631-c01c64a7a4f6 | 著者3 |        2
-- (4 rows)
-- 実行計画
--                                                          QUERY PLAN
-- -----------------------------------------------------------------------------------------------------------------------------
--  Nested Loop  (cost=0.34..226.84 rows=1000 width=64)
--    ->  Index Only Scan using book_authors_pkey on book_authors  (cost=0.17..29.84 rows=1000 width=32)
--          Index Cond: (book_id = ANY ('{1fb112d1-54c9-4308-99c6-0163bfd0172d,554bc347-f36c-4766-b66f-d651c84c56ba}'::uuid[]))
--    ->  Index Scan using authors_pkey on authors  (cost=0.17..0.20 rows=1 width=48)
--          Index Cond: (author_id = book_authors.author_id)
-- (5 rows)



-- authorsをバッチ取得 - パターン2
-- 無理やりauthorsの情報は被らないようにしたが、2d arrayを取得することになるので複雑で、
-- パフォーマンスも悪い
select
  authors.*,
  array( -- どのbook_idに紐づいたauthorかを判別するために必要
    select
      array[book_authors.book_id::text, book_authors.position::text]
    from
      book_authors
    where
      book_authors.author_id = authors.author_id
  ) as book_ids
from 
  authors
where
  authors.author_id in (
    select book_authors.author_id
    from book_authors
    where
      book_authors.book_id in ('1FB112D1-54C9-4308-99C6-0163BFD0172D', '554BC347-F36C-4766-B66F-D651C84C56BA')
    );

-- 結果
--               author_id               | name  |                                      book_ids
-- --------------------------------------+-------+-------------------------------------------------------------------------------------
--  bed827ff-6847-41bd-88a0-b77fdd74bea3 | 著者1 | {{1fb112d1-54c9-4308-99c6-0163bfd0172d,1},{554bc347-f36c-4766-b66f-d651c84c56ba,1}}
--  467021fd-ae39-4ba0-bc7b-3e5b21ef69f9 | 著者2 | {{1fb112d1-54c9-4308-99c6-0163bfd0172d,2}}
--  f66c5c85-5044-4433-b631-c01c64a7a4f6 | 著者3 | {{554bc347-f36c-4766-b66f-d651c84c56ba,2}}
-- (3 rows)

--                                                                QUERY PLAN                                              
-- -----------------------------------------------------------------------------------------------------------------------------------------
--  Hash Semi Join  (cost=19.26..221.37 rows=13 width=80)
--    Hash Cond: (authors.author_id = book_authors.author_id)
--    ->  Seq Scan on authors  (cost=0.00..20.70 rows=1070 width=48)
--    ->  Hash  (cost=19.09..19.09 rows=13 width=16)
--          ->  Bitmap Heap Scan on book_authors  (cost=8.40..19.09 rows=13 width=16)
--                Recheck Cond: (book_id = ANY ('{1fb112d1-54c9-4308-99c6-0163bfd0172d,554bc347-f36c-4766-b66f-d651c84c56ba}'::uuid[]))
--                ->  Bitmap Index Scan on book_authors_pkey  (cost=0.00..8.39 rows=13 width=0)
--                      Index Cond: (book_id = ANY ('{1fb112d1-54c9-4308-99c6-0163bfd0172d,554bc347-f36c-4766-b66f-d651c84c56ba}'::uuid[]))
--    SubPlan 1
--      ->  Bitmap Heap Scan on book_authors book_authors_1  (cost=4.20..13.73 rows=6 width=32)
--            Recheck Cond: (author_id = authors.author_id)
--            ->  Bitmap Index Scan on book_authors_author_id_idx  (cost=0.00..4.20 rows=6 width=0)
--                  Index Cond: (author_id = authors.author_id)
-- (13 rows)


-- authorsをバッチ取得 - パターン3
-- 今のところベスト
select
  authors.author_id,
  authors.name,
  array_agg(
      array[book_authors.book_id::text, book_authors.position::text]
   ) as book_ids
from 
  authors
  join book_authors using (author_id)
where
      book_authors.book_id in ('1FB112D1-54C9-4308-99C6-0163BFD0172D', '554BC347-F36C-4766-B66F-D651C84C56BA')
group by authors.author_id, authors.name;

-- 結果
--               author_id               | name  |                                      book_ids
-- --------------------------------------+-------+-------------------------------------------------------------------------------------
--  467021fd-ae39-4ba0-bc7b-3e5b21ef69f9 | 著者2 | {{1fb112d1-54c9-4308-99c6-0163bfd0172d,2}}
--  f66c5c85-5044-4433-b631-c01c64a7a4f6 | 著者3 | {{554bc347-f36c-4766-b66f-d651c84c56ba,2}}
--  bed827ff-6847-41bd-88a0-b77fdd74bea3 | 著者1 | {{1fb112d1-54c9-4308-99c6-0163bfd0172d,1},{554bc347-f36c-4766-b66f-d651c84c56ba,1}}
-- (3 rows)

-- 実行計画
--                                                             QUERY PLAN
-- -----------------------------------------------------------------------------------------------------------------------------------
--  HashAggregate  (cost=45.32..46.57 rows=100 width=80)
--    Group Key: authors.author_id
--    ->  Nested Loop  (cost=0.32..43.82 rows=100 width=68)
--          ->  Index Scan using book_authors_pkey on book_authors  (cost=0.16..14.07 rows=100 width=36)
--                Index Cond: (book_id = ANY ('{1fb112d1-54c9-4308-99c6-0163bfd0172d,554bc347-f36c-4766-b66f-d651c84c56ba}'::uuid[]))
--          ->  Index Scan using authors_pkey on authors  (cost=0.16..0.30 rows=1 width=48)
--                Index Cond: (author_id = book_authors.author_id)
-- (7 rows)

