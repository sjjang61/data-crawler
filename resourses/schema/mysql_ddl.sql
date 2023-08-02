-- 200601 ~ 202301
CREATE TABLE `apartment_trade` (

    `price` int NOT NULL COMMENT '거래금액',
    `tr_type` varchar(16) NULL COMMENT '거래유형(중개거래,직거래)',
    `birth_year` int NULL COMMENT '건축년도',
    `bubjungdong` varchar(32) NULL COMMENT '법정동',
    `name` varchar(64) NOT NULL COMMENT '아파트',

    `year` int NOT NULL COMMENT '거래년',
    `month` tinyint not NULL COMMENT '거래월',
    `day` tinyint not NULL COMMENT '거래일',

    `size` float NOT NULL COMMENT '전용면적',
    `trader_loc` varchar(32) NULL COMMENT '중개사소재지',
    `jibun` varchar(32) NULL COMMENT '지번',
    `loc_cd` char(5) NOT NULL COMMENT '지역코드',
    `layer` int not NULL COMMENT '층',

    `cancel_date` datetime NULL COMMENT '해제사유발생일',
    `cancel_yn` char(1) NULL COMMENT '해제여부',
    `reg_date` datetime default now() COMMENT '등록시간',

    PRIMARY KEY ( year, month, day, loc_cd, name, size, layer, price )
)
COMMENT '아파트 매매(실거래) 정보';


CREATE TABLE `apartment_rent` (

    `req_yn` varchar(32) NULL COMMENT '갱신요구권사용',
    `birth_year` int NULL COMMENT '건축년도',
    `rent_type` varchar(16) NULL COMMENT '계약유형(전세,월세)',
    `duration` varchar(32) NULL COMMENT '계약기간',

    `bubjungdong` varchar(32) NULL COMMENT '법정동',
    `deposit_price` int NULL COMMENT '보증금',
    `rental_price` int NULL COMMENT '월세금액',
    `name` varchar(64) NOT NULL COMMENT '아파트',

    `year` int NOT NULL COMMENT '계약년',
    `month` tinyint not NULL COMMENT '계약월',
    `day` tinyint not NULL COMMENT '계약일',
    `size` float NOT NULL COMMENT '전용면적',

    `prev_deposit_price` int NULL COMMENT '종전계약보증금',
    `prev_rental_price` int NULL COMMENT '종전계약월세',
    `jibun` varchar(32) NULL COMMENT '지번',
    `loc_cd` char(5) NOT NULL COMMENT '지역코드',
    `layer` int NULL COMMENT '층',
    `reg_date` datetime default now() COMMENT '등록시간'
)
COMMENT '아파트 전월세 거래정보';
CREATE INDEX apartment_rent_idx1 on apartment_rent (  year, month, day, loc_cd, name, size, layer );



CREATE TABLE `land_trade` (

    `price` int NOT NULL COMMENT '거래금액',
    `ct_type` varchar(16) NULL COMMENT '거래유형(중개거래,직거래)',
    `ct_type2` varchar(16) NULL COMMENT '구분(지분)',
    `bubjungdong` varchar(32) NULL COMMENT '법정동',
    `sido` varchar(32) NULL COMMENT '시군구',

    `year` int NOT NULL COMMENT '거래년',
    `month` tinyint not NULL COMMENT '거래월',
    `day` tinyint not NULL COMMENT '거래일',

    `size` float NOT NULL COMMENT '거래면적',
    `purpose` varchar(32) NULL COMMENT '용도지역',
    `jibun` varchar(32) NULL COMMENT '지번',
    `jimok` varchar(32) NULL COMMENT '지목',
    `loc_cd` char(5) NOT NULL COMMENT '지역코드',
    `trader_loc` varchar(32) NULL COMMENT '중개사소재지',

    `cancel_date` datetime NULL COMMENT '해제사유발생일',
    `cancel_yn` char(1) NULL COMMENT '해제여부',
    `reg_date` datetime default now() COMMENT '등록시간'
)
COMMENT '토지 매매(실거래) 정보';
CREATE INDEX land_trade_idx1 on land_trade (  year, month, day, sido, bubjungdong, loc_cd );


CREATE TABLE `store_trade` (

    `price` int NOT NULL COMMENT '거래금액',
    `ct_type` varchar(16) NULL COMMENT '거래유형(중개거래,직거래)',
    `ct_type2` varchar(16) NULL COMMENT '유형',
    `birth_year` int NULL COMMENT '건축년도',

    `bubjungdong` varchar(32) NULL COMMENT '법정동',
    `sido` varchar(32) NULL COMMENT '시군구',

    `year` int NOT NULL COMMENT '거래년',
    `month` tinyint not NULL COMMENT '거래월',
    `day` tinyint not NULL COMMENT '거래일',
    `layer` int NULL COMMENT '층',

    `building_size` float NOT NULL COMMENT '건물면적',
    `land_size` float NULL COMMENT '대지면적',
    `building_purpose` varchar(32) NULL COMMENT '건물주용도',
    `land_purpose` varchar(32) NULL COMMENT '용도지역',

    `jibun` varchar(32) NULL COMMENT '지번',
    `loc_cd` char(5) NOT NULL COMMENT '지역코드',
    `trader_loc` varchar(32) NULL COMMENT '중개사소재지',

    `cancel_date` datetime NULL COMMENT '해제사유발생일',
    `cancel_yn` char(1) NULL COMMENT '해제여부',
    `reg_date` datetime default now() COMMENT '등록시간'
)
COMMENT '상업용부동산 매매 정보';
CREATE INDEX store_trade_idx1 on store_trade (  year, month, day, loc_cd, sido, bubjungdong,  jibun );


CREATE TABLE `apartment_trade_rent_rate` (
  `name` varchar(32) DEFAULT NULL,
  `trade_date` datetime DEFAULT NULL,
  `rent_date` datetime DEFAULT NULL,
  `size` float DEFAULT NULL,
  `trade_price` int(11) DEFAULT NULL,
  `rent_price` int(11) DEFAULT NULL,
  `rent_rate` float DEFAULT NULL,
  `bubjungdong` varchar(45) DEFAULT NULL,
  `jibun` varchar(45) DEFAULT NULL,
  `loc_cd` varchar(45) DEFAULT NULL,
  `reg_ymdt` datetime DEFAULT NULL
)

comment '전세가비율';
CREATE INDEX apartment_trade_rent_rate_idx1 on apartment_trade_rent_rate (  bubjungdong, name, size );
CREATE INDEX apartment_trade_rent_rate_idx2 on apartment_trade_rent_rate (  trade_date, loc_cd );
CREATE INDEX apartment_trade_rent_rate_idx3 on apartment_trade_rent_rate (  rent_date, loc_cd );

-- 매매가 마지막 데이터 추가
insert into apartment_trade_rent_rate
SELECT
		a.name, a.trade_date, null, a.size, a.price, null, null, a.bubjungdong,  a.jibun, a.loc_cd, now()
FROM (
	select *, str_to_date(concat( tr_year, '-', tr_month, '-', tr_day ), '%Y-%m-%d') trade_date
	from nextu.apartment_trade
	where tr_year >= 2020 and loc_cd like '11%' and cancel_date is null
) a,
(
	select bubjungdong, name, size, max(str_to_date(concat( tr_year, '-', tr_month, '-', tr_day ), '%Y-%m-%d')) last_trade_date
	from nextu.apartment_trade
	where tr_year >= 2020 and loc_cd like '11%' and cancel_date is null
    group by bubjungdong, name, size
) last_row
where a.bubjungdong = last_row.bubjungdong and a.name = last_row.name and a.size = last_row.size and a.trade_date = last_row.last_trade_date
order by a.name, a.size, a.tr_year, a.tr_month, a.tr_day;



-- 전세가 업데이트
update apartment_trade_rent_rate a,
(
    SELECT
        a.name, a.rent_date, null, a.size, a.deposit_price, a.bubjungdong, a.jibun, a.loc_cd
	FROM (
        select *, str_to_date(concat( year, '-', month, '-', day ), '%Y-%m-%d') rent_date
        from nextu.apartment_rent
        where year >= 2020 and loc_cd like '11%'
    ) a, (
		select bubjungdong, name, size, max(str_to_date(concat(year, '-', month, '-', day ), '%Y-%m-%d')) last_rent_date
        from nextu.apartment_rent
        where year >= 2020 and loc_cd like '11%'
        group by bubjungdong, name, size
	) last_row
    where a.bubjungdong = last_row.bubjungdong and a.name = last_row.name and a.size = last_row.size and a.rent_date = last_row.last_rent_date
    order by name, size, year, month, day
) st

set
    a.rent_date = st.rent_date,
    a.rent_price = st.deposit_price,
    a.rent_rate = round( st.deposit_price / a.trade_price, 4 )

where a.bubjungdong = st.bubjungdong and a.size = st.size and a.name = st.name and a.jibun = st.jibun;


