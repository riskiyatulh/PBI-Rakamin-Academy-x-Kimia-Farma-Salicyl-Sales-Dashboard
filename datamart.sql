-- CREATE TABLE
-- penjualan
CREATE TABLE penjualan (
	id_distributor character varying(50) NULL,
	id_cabang character varying(50) NULL,
	id_invoice character varying(50) NOT NULL,
	tanggal date NULL,
	id_customer character varying(50) NULL,
	id_barang character varying(50) NULL,
	jumlah_barang integer NULL,
	unit character varying(50) NULL,
	harga float8 NULL,
	mata_uang character varying(50) NULL,
	brand_id character varying(50) NULL,
	lini character varying(50) NULL,
	CONSTRAINT penjualan_pkey PRIMARY KEY (id_invoice)
);

-- pelanggan
CREATE TABLE pelanggan (
	id_customer character varying (50) NULL,
	levell character varying (50) NULL,
	nama character varying (50) NULL,
	id_cabang_sales character varying (50) NULL,
	cabang_sales character varying (50) NULL,
	id_group character varying (50) NULL,
	groupp character varying (50) NULL
);

-- barang
CREATE TABLE barang (
	kode_barang character varying (50) NULL,
	sektor character varying (50) NULL,
	nama_barang character varying (50) NULL,
	tipe character varying (50) NULL,
	nama_tipe character varying (50) NULL,
	kode_lini integer NULL,
	lini character varying (50) NULL,
	kemasan character varying (50) NULL,
	CONSTRAINT barang_pkey PRIMARY KEY (kode_barang)
);

-- INPUT DATA CSV
-- penjualan
COPY penjualan (
	id_distributor,
	id_cabang,
	id_invoice,
	tanggal,
	id_customer,
	id_barang,
	jumlah_barang,
	unit,
	harga,
	mata_uang,
	brand_id,
	lini)
FROM 'E:\Bootcamp Data Science\Rakamin\PBI Kimia Farma\penjualan.csv'
DELIMITER ','
CSV HEADER;
SELECT * FROM penjualan;

-- pelanggan
COPY pelanggan (
	id_customer,
	levell,
	nama,
	id_cabang_sales,
	cabang_sales,
	id_group,
	groupp)
FROM 'E:\Bootcamp Data Science\Rakamin\PBI Kimia Farma\pelanggan.csv'
DELIMITER ','
CSV HEADER;
SELECT * FROM pelanggan;

-- barang
COPY barang (
	kode_barang,
	sektor,
	nama_barang,
	tipe,
	nama_tipe,
	kode_lini,
	lini,
	kemasan)
FROM 'E:\Bootcamp Data Science\Rakamin\PBI Kimia Farma\barang.csv'
DELIMITER ','
CSV HEADER;
SELECT * FROM barang;

-- Design Datamart
-- Tabel base
CREATE TABLE base_table AS
SELECT 
	pj.id_invoice,
	pj.tanggal,
	pj.jumlah_barang,
	pj.unit,
	pj.harga, 
	pl.nama,
	pl.cabang_sales,
	pl.groupp,
	b.nama_barang,
	pj.lini
FROM penjualan AS pj
JOIN pelanggan AS pl ON pj.id_customer = pl.id_customer
JOIN barang AS b ON pj.id_barang = b.kode_barang;

-- Tabel Aggregat
-- Revenue based on nama barang
CREATE TABLE aggregate_table AS
SELECT * 
FROM (
    SELECT 
        pj.id_invoice,
        b.nama_barang,
		b.lini,
		pj.tanggal,
		TO_CHAR(pj.tanggal, 'Month') AS bulan,
		pj.jumlah_barang,
		pj.harga,
		ROUND(pj.harga * pj.jumlah_barang) AS revenue,
        pl.nama,
        pl.cabang_sales
    FROM penjualan AS pj
    JOIN pelanggan AS pl ON pj.id_customer = pl.id_customer
    JOIN barang AS b ON pj.id_barang = b.kode_barang
    GROUP BY 1, 2, 3, 9, 10
    ORDER BY 7 DESC
) AS product_revenue;

-- Monthly sales
CREATE TABLE monthly_sales AS
WITH monthly_sales AS (
    SELECT
        EXTRACT(MONTH FROM agg.tanggal),
        agg.bulan,
		agg.cabang_sales,
        agg.nama,
        agg.nama_barang,
        agg.lini,
        SUM(agg.jumlah_barang) AS total_qty,
        SUM(agg.revenue) AS total_revenue,
        SUM(SUM(agg.revenue)) OVER(PARTITION BY EXTRACT(MONTH FROM agg.tanggal), agg.cabang_sales) AS branch_total_monthly_sales
    FROM aggregate_table AS agg
    GROUP BY 1, 2, 3, 4, 5, 6
    ORDER BY 1, 2, 4 DESC
)
SELECT * FROM monthly_sales;
