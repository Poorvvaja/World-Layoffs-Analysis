/* DATA CLEANING AND EXPLORATORY DATA ANALYTICS PROJECT, WORLD LAYOFFS DATABASE, POORVVAJA R */

-- DATA CLEANING 

SELECT * FROM layoffs;

-- 1. Removing Duplicates from the Data
-- 2. Standardizing the Data 
-- 3. Populating NULL and blank values if possible
-- 4. Removing irrelavent columns, if any 

CREATE TABLE layoffs_staging	 #to not make any changes to the original set of values, best practice not to work on real raw data
LIKE layoffs;

INSERT layoffs_staging 
SELECT * FROM layoffs; 

SELECT * FROM layoffs_staging;

-- Adding row numbers to facilitate finding duplicates

# if there is 2 or above in the row_num, then there are duplicates 

WITH duplicate_cte AS 
( 
SELECT *,
ROW_NUMBER() 
OVER ( PARTITION BY company,location, industry, total_laid_off, percentage_laid_off,`date`,stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT * FROM duplicate_cte 
WHERE row_num > 1;					# We cannot delete here itself because any type of update is not possible in CTE


SELECT * FROM layoffs_staging
WHERE company = 'Casper';			#sample checking duplicates

-- We create another table with row_num as extra row, and delete duplicate, i.e, row_num > 1
-- copied the create statement of layoffs_staging from SCHEMAS Navigator

CREATE TABLE `layoffs_staging2`
 (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int											#Adding this column 			
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() 
OVER ( PARTITION BY company,location, industry, total_laid_off, percentage_laid_off,`date`,stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

SELECT * FROM layoffs_staging2
WHERE row_num > 1;

DELETE FROM layoffs_staging2
WHERE row_num > 1;					# Removed Duplicates

-- Standardizing the Data: Finding issues and fixing it

UPDATE layoffs_staging2
SET company = TRIM(company);		# Removes blank spaces in the front and back of company names

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;							# We notice that there are different formatted names for Crypto

SELECT * FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2 
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';		#Updated 3 rows

SELECT DISTINCT location 
FROM layoffs_staging2
ORDER BY 1;							# Checking if anything needs to be updated, no issues found.

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;							# United States has been formatted in two different ways

UPDATE layoffs_staging2 
SET country = TRIM( TRAILING '.' FROM country) 		#To remove the trailing '.'
WHERE country LIKE 'United States%';

-- The date column is in text datatype, it needs to be changed to DATE

SELECT `date`, STR_TO_DATE(`date`, '%m/%d/%y' )
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%y' );

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE; 			# Changing the datatype to Date

-- Working with NULLs and BLANKs

SELECT * FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL; 	# If both are null, there's no point with the data

UPDATE layoffs_staging2
SET industry = NULL 
WHERE industry = '';				# Changing blank values to NULL for easy further access and updates
									# Shows error as i tried to update a table without a WHERE that uses a key column
                                    
SET SQL_SAFE_UPDATES = 0;			# Disabling Safe update mode for the current session

UPDATE layoffs_staging2
SET industry = NULL 
WHERE industry = '';				# Now three matched rows have been changed

SELECT * FROM layoffs_staging2
WHERE industry = NULL;				# We find four companies where industry is null
									# Airbnb, Bally's Interactive, Carvana, Juul
							
SELECT * FROM layoffs_staging2
WHERE company = 'Airbnb';			# There's other row with Airbnb with industry specified as Travel

SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2			# Joining same table with itself
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE (t1.industry IS NULL)
AND t2.industry IS NOT NULL;		# Except Bally's, other rows with industries as NULL are filled
									# Bally's has only one row, doesnt have a row where industry is NOT NULL
                                    

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry		# Nulls or in t1.industry will be filled with corresponding t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- With the data we have, we cannot fill the NULLs and blanks in total _laid_off and percentage_laid_off

-- With rows with no values in both total and percentage laid off, we do not need those rows for layoffs

DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;  	# 362 ROWS HAVE BEEN DELETED

-- Deleting the column row_num as it is comething we created to find duplicates

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT * FROM layoffs_staging2;


-- EXPLORATORY DATA ANALYSIS


SELECT * FROM layoffs_staging2;

SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2;

SELECT * FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC;

SELECT * FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

SELECT company, SUM(total_laid_off) 
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;				 		#Which companies had the maximum layoffs

SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;

SELECT industry, SUM(total_laid_off) 
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;						# Which industries got hit the most during layoffs

SELECT country, SUM(total_laid_off) 
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC; 						# In which coutries were the max number of people laid off from?

SELECT stage, SUM(total_laid_off) 
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;

SELECT company, SUM(percentage_laid_off) 	# Percentage here is not very relevant because we have a total numbers
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

SELECT SUBSTR(`DATE`, 1,7) AS `MONTH`, SUM(total_laid_off)
FROM layoffs_staging2
WHERE SUBSTR(`DATE`, 1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC;								# Shows totals of every month, every year
											# We need a rolling total of this data
                                            
select * from layoffs_staging2;
update layoffs_staging2
set `date` = STR_TO_DATE(`date`, '%m/%d/%Y');	

WITH rolling_total AS								# to find rolling total, cte
(
SELECT SUBSTR(`DATE`, 1,7) AS `MONTH`, SUM(total_laid_off) as total_off
FROM layoffs_staging2
WHERE SUBSTR(`DATE`, 1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC
)
SELECT `MONTH`, SUM(total_off) OVER (ORDER BY `MONTH`) AS rolling_total
FROM rolling_total;




SELECT company,YEAR(`date`),  SUM(total_laid_off) 	
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY company ASC;					# To see how many layoffs a company did each year

-- Ranking the layoffs to see who laid off most people per year, with CTE

WITH company_year (company, years, layoffs) AS
(
SELECT company, YEAR(`date`),  SUM(total_laid_off) 	
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
)
SELECT *, dense_rank() OVER (PARTITION BY years order by layoffs desc) AS ranking
FROM company_year
WHERE years IS NOT NULL
ORDER BY ranking asc;			#We can see the maximum layoff and which company did that, in every year 2020 to 2023
								# year, company which did the maximum layoff that year and how many people they laid off





	










					










