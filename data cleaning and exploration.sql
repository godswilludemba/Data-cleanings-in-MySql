-- Data cleaning
SELECT * 
FROM layoffs;

-- 1. remove Duplicates
-- 2. Standardize the Data
-- 3. Null Value or Blank Values
-- 4. Remove Any Columns or Rows

-- CREATE A STAGING TABLE
CREATE TABLE layoffs_staging
LIKE layoffs;

-- VIEW THE STAGING DATA
SELECT * 
FROM layoffs_staging;

-- INSERT DATA FROM RAW TABLE IN TO THE STAGING TABLE
INSERT layoffs_staging
SELECT *
FROM layoffs;

-- INSERT ROW NUMBER TO THE STATGING
-- this code intro a unique id to each row of the data set in the table

 SELECT *,
ROW_NUMBER() OVER(
partition by company, industry, total_laid_off, percentage_laid_off,`date`,
stage, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- to work effectively with this code, we create a CTE (Comon Table Expression. is A temprary result for reference)
 WITH duplicate_cte AS
 (
 SELECT *,
ROW_NUMBER() OVER(
partition by company, industry, total_laid_off, percentage_laid_off,`date`,
stage, funds_raised_millions ) AS row_num
FROM layoffs_staging
 )
 SELECT *
 FROM duplicate_cte
 WHERE row_num > 1;
 
 -- Select to check if the data highlighted as duplicate are actually duplicate
 
 SELECT * 
FROM layoffs_staging
WHERE company ='Casper';
 
-- havingidentified some data with a duplicate value and because we can not delete from a staging file directly, we create
-- a new staging file called staging2from the old staging 

CREATE TABLE `layoffs_staging2` (
  `company` text DEFAULT NULL,
  `location` text DEFAULT NULL,
  `industry` text DEFAULT NULL,
  `total_laid_off` int(11) DEFAULT NULL,
  `percentage_laid_off` text DEFAULT NULL,
  `date` text DEFAULT NULL,
  `stage` text DEFAULT NULL,
  `country` text DEFAULT NULL,
  `funds_raised_millions` int(11) DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- SELECT to check if created
SELECT *
FROM layoffs_staging2;

-- Insert in to the new staging
INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
partition by company, industry, total_laid_off, percentage_laid_off,`date`,
stage, funds_raised_millions ) AS row_num
FROM layoffs_staging;

-- select to confirm if it was inserted along with the row_num and filter
SELECT *
FROM layoffs_staging2
WHERE row_num >1
;

-- delete the duplicate colums
DELETE 
FROM layoffs_staging2
WHERE row_num >1;

-- reselect to check table
SELECT *
FROM layoffs_staging2;

-- STANDARDIZING DATA

-- this is basically finding issues in your data and fixing it, caps issues and others
-- check for the trimable data

SELECT company, TRIM(company)
FROM layoffs_staging2;

-- update on our file
UPDATE layoffs_staging2
SET company = TRIM(company);

-- select another filed. check for duplicate data by introducting DISTINCT

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

-- search for industry like crypto
SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'crypto%'
;

-- UPDATE in this case crypto
UPDATE layoffs_staging2
SET industry = 'crypto'
WHERE industry LIKE 'crypto%';

-- confirm the update
SELECT DISTINCT industry
FROM layoffs_staging2;

-- country
-- most times its good to inspect al the variables

-- specifics
SELECT distinct(country)
FROM layoffs_staging2;

-- search

SELECT *
FROM layoffs_staging2
WHERE country LIKE 'United States%'
ORDER BY 1;

-- TO REMOVE COMA or full stop AT THE END OF a misspelt variable we combine DISTINCT, TRIM AND TRAILING
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

-- UPDATE
UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';
-- reconfirm
SELECT country
FROM layoffs_staging2;

-- for date , is always good to change it to a daytime as against text expecially when working with time series
-- THAT IS STRING TO DATE (STR_TO_DATE())
SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

-- UPDATE
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- reconfirm
SELECT `date`
FROM layoffs_staging2;

-- change the data type to date using ALTER TABLE CREATION
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- check for null row
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS Null;

-- check for distict varabes/field with missing values and nulls
UPDATE layoffs_staging2
SET industry = null
WHERE industry = ''
;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

-- working on blank field using Airbnb

SELECT *
FROM layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;


UPDATE layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
	ON t1.company = t2.company
SET t1.industry =t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- deleting column with a fake or incorrect data like null with out corespondent
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- delete the nulls as observed
DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2;

-- Droping a column(s) from the table when we don't need them or when not usful any more.
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;