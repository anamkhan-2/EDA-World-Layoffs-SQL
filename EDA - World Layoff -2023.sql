													-- Data Cleaning --
                                                    
-- 1. remove duplicates
-- 2. standardize the dara
-- 3. remove null, blank values
-- 4. remove useless columns


-- truning off safe update mode
SET SQL_SAFE_UPDATES = 0;

                                              -- Step 1
							
-- removing duplicates


-- imported data layoffs
select * 
FROM layoffs;
select * from layoffs_stagging; 
CREATE TABLE layoffs_stagging -- creating replica off layoffs table
LIKE layoffs;

INSERT layoffs_stagging -- inserting exact same values as layoff
Select * from layoffs;

-- creating cte to assign row num as an identifier for duplicates
WITH duplicate_cte AS
(
SELECT * , 
row_number() OVER( partition by 
company,location,industry,total_laid_off,
percentage_laid_off,'date',stage,country,funds_raised_millions) AS row_num
from layoffs_stagging
)

-- checking for duplicates
SELECT * FROM duplicate_cte
where row_num >= 2;

-- individual double check for duplicates.
select * from layoffs_stagging where company = 'Casper';

-- making another table so that we can delete duplicates
CREATE TABLE `layoffs_stagging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
   `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * from layoffs_stagging2;

-- copying exact same data from layoffs_stagging
insert into layoffs_stagging2 
SELECT * , 
row_number() OVER( partition by 
company,location,industry,total_laid_off,
percentage_laid_off,'date',stage,country,funds_raised_millions) AS row_num
from layoffs_stagging;

-- deleting all duplicates
delete from layoffs_stagging2 	WHERE row_num >= 2;

                      -- Step 2
                      
-- Standardizing the data

-- removing any extra space at the begining of company names
update layoffs_stagging
set company=trim(company);

select Distinct industry from layoffs_stagging2 order by 1;

-- checking for industries which can be standardize
select * from layoffs_stagging2 where industry LIKE 'Crypto%';

-- standardizing industry by giving one distinct name to alikes
update layoffs_stagging2
set industry='Crypto'
where industry LIKE 'Crypto%';


select country from layoffs_stagging2
where country = 'United States.';
update layoffs_stagging2

-- removing '.' at the end by help of trailing 
set country=Trim(Trailing '.' from country)
where country='United%';
select distinct country, trim(trailing '.' from country)
from layoffs_stagging2 order by 1;

-- changing date from text to date format

UPDATE layoffs_stagging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');
select date from layoffs_stagging2;

                                   -- Step 3
-- removing nulls and ''

select * from layoffs_stagging2 where industry IS NULL OR industry = '' order by industry;

-- checking individually that if any of its data exists in system
select * from layoffs_stagging2 where company='Juul';

-- setting all to default nulls for easy handling
update layoffs_stagging2 
set industry = null where industry='';

-- populating the industry by using its own existing data
update layoffs_stagging2 t1
join layoffs_stagging2 t2
on t1.company=t2.company
And t1.location=t2.location
set t1.industry=t2.industry
where t1.industry is null
and t2.industry is not null;

                                 
                                 -- Step 4
                                 
-- removing useless columns and rows

-- removing row_num bcz we dont need it anymore
alter table layoffs_stagging2 
drop column row_num;

-- checking for data that is useless bcz it has both nulls
select * from layoffs_stagging2 where total_laid_off IS null AND percentage_laid_off IS NULL;

-- removing useless data to get properly cleaned data
delete from layoffs_stagging2 where total_laid_off IS null AND percentage_laid_off IS NULL;



							-- -- EDA -- -- Exploratory Data Analysis -- --
                  
	-- finding company's total laid off with dates
   select `date`, company,max(total_laid_off)
   from layoffs_stagging2
   group by `date`, company
   order by 1 desc;
   
   -- sum of total laid off
   select sum(total_laid_off)
   from layoffs_stagging2;
   
   -- finding company that laid off max along industry and date
   select company, industry , max(total_laid_off), `date`
   from layoffs_stagging2
   group by company, industry , `date`
   order by `date` desc , max(total_laid_off) desc;
   
   -- finding dates on which layoff started and ended
   select min(`date`), max(`date`)
   from layoffs_stagging2;
   
   -- finding which country laid off most
   select country, sum(total_laid_off)
   from layoffs_stagging2
   group by country
   order by sum(total_laid_off) desc;					

-- finding by year 
 select YEAR(`date`), sum(total_laid_off)
   from layoffs_stagging2 group by year(`date`)
order by 1 desc;  

-- finding by month
select substring(`date`, 1 ,7) as months ,sum(total_laid_off)
from layoffs_stagging2
group by months
order by 1 desc ;

-- count of rows of laid off
select count(total_laid_off)
from layoffs_stagging2;

-- finding only 1 company that laid off most
select company, max(total_laid_off),`date`
from layoffs_stagging2
group by company,`date`
order by 2 desc
limit 1 ;

-- avg of laid off percentage
select avg(percentage_laid_off) as lay_off
from layoffs_stagging2;

-- finding 1st five companies in each year to lay off max
WITH Company_Year AS 
(
  SELECT company, YEAR(date) AS years, SUM(total_laid_off) AS total_laid_off
  FROM layoffs_stagging2
  GROUP BY company, YEAR(date)
)
, Company_Year_Rank AS (
  SELECT company, years, total_laid_off, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
  FROM Company_Year
)
SELECT company, years, total_laid_off, ranking
FROM Company_Year_Rank
WHERE ranking <= 5
AND years IS NOT NULL
ORDER BY years ASC, total_laid_off DESC;


-- rolling sum of layoffs with each year
WITH DATE_CTE AS 
(
SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS s_total_laid_off
FROM layoffs_stagging2
GROUP BY dates
ORDER BY dates ASC
)
SELECT dates, SUM(s_total_laid_off) OVER (ORDER BY dates ASC) as rolling_total_layoffs, s_total_laid_off
FROM DATE_CTE
ORDER BY dates ASC;


