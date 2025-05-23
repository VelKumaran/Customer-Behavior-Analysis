#1)Drop offf
SELECT Stage, COUNT(*) AS DropOffCount
FROM Customer_Journey
WHERE Action = 'Drop-off'
GROUP BY Stage
ORDER BY DropOffCount DESC;  


#2)avg duration
SELECT Stage, ROUND(AVG(Duration), 2) AS AvgDuration
FROM customer_journey
GROUP BY Stage
ORDER BY AvgDuration DESC;


#3)retention rate
SELECT 
    (COUNT(DISTINCT CASE WHEN VisitDate >= DATE_SUB(CURDATE(), INTERVAL 1 YEAR) THEN CustomerID END) 
     / COUNT(DISTINCT CustomerID)) * 100 AS RetentionRate
FROM Customer_journey;

#4)highest rated
SELECT p.ProductName, 
       ROUND(AVG(cr.Rating), 2) AS AvgRating, 
       COUNT(*) AS TotalReviews
FROM Customer_reviews cr
JOIN Products p ON cr.ProductID = p.ProductID
GROUP BY p.ProductName
ORDER BY AvgRating DESC
LIMIT 5;

#)lowest rated
SELECT p.ProductName, 
       ROUND(AVG(cr.Rating), 2) AS AvgRating, 
       COUNT(*) AS TotalReviews
FROM CustomerReviews cr
JOIN Products p ON cr.ProductID = p.ProductID
GROUP BY p.ProductName
ORDER BY AvgRating ASC
LIMIT 5;

#5action leading to successful conversion
WITH engagement_analysis AS (
    SELECT 
        e.EngagementID, 
        e.ProductID, 
        e.ContentType, 
        e.Likes, 
        e.ViewsClicksCombined, 
        cj.Stage, 
        cj.Action AS UserAction
    FROM engagement_data e
    LEFT JOIN customer_journey cj ON e.ProductID = cj.ProductID
)
SELECT 
    ContentType,
    COUNT(DISTINCT EngagementID) AS Total_Engagements,
    SUM(Likes) AS Total_Likes,
    SUM(CASE WHEN Stage = 'Checkout' AND UserAction = 'Purchase' THEN 1 ELSE 0 END) AS Total_Purchases,
    ROUND(
        (SUM(CASE WHEN Stage = 'Checkout' AND UserAction = 'Purchase' THEN 1 ELSE 0 END) * 100.0) / COUNT(DISTINCT EngagementID), 2
    ) AS Conversion_Rate
FROM engagement_analysis
GROUP BY ContentType
ORDER BY Conversion_Rate DESC;

#6sentiment analysis
WITH review_analysis AS (
    SELECT 
        cr.ReviewID, 
        cr.CustomerID, 
        cr.ProductID, 
        p.ProductName, 
        cr.Rating, 
        cr.ReviewText,
        CASE 
            WHEN Rating <= 2 THEN 'Negative'
            WHEN Rating = 3 THEN 'Neutral'
            ELSE 'Positive'
        END AS Sentiment
    FROM customer_reviews cr
    JOIN products p ON cr.ProductID = p.ProductID
)
-- Customer Reviews Sentiment & Satisfaction Trends


SELECT 
    Sentiment,
    COUNT(*) AS Total_Reviews,
    ROUND(AVG(Rating), 2) AS Avg_Rating
FROM review_analysis
GROUP BY Sentiment
ORDER BY Avg_Rating DESC;



#7review trends with product perfomance
SELECT p.ProductID, p.ProductName, COUNT(r.ReviewID) AS ReviewCount, 
       AVG(r.Rating) AS AvgRating
FROM customer_reviews r
JOIN products p ON r.ProductID = p.ProductID
GROUP BY p.ProductID, p.ProductName
ORDER BY ReviewCount DESC;


#Finding Common Actions Leading to Successful Conversions

# Customer Demographics & Conversion Analysis
#Find out which customer segments have the highest conversion rates.


WITH customer_conversion_age AS (
    SELECT 
        CASE 
            WHEN c.Age < 30 THEN '<30'
            WHEN c.Age BETWEEN 30 AND 45 THEN '30-45'
            ELSE '>45'
        END AS Age_Range,
        COUNT(DISTINCT cj.CustomerID) AS Total_Customers,
        SUM(CASE WHEN cj.Stage = 'Checkout' AND cj.`Action` = 'Purchase' THEN 1 ELSE 0 END) AS Total_Purchases,
        ROUND(
            (SUM(CASE WHEN cj.Stage = 'Checkout' AND cj.`Action` = 'Purchase' THEN 1 ELSE 0 END) * 100.0) / COUNT(DISTINCT cj.CustomerID), 2
        ) AS Conversion_Rate
    FROM customer_journey cj
    JOIN customers c ON cj.CustomerID = c.CustomerID
    GROUP BY Age_Range
),

customer_conversion_gender AS (
    SELECT 
        c.Gender,
        COUNT(DISTINCT cj.CustomerID) AS Total_Customers,
        SUM(CASE WHEN cj.Stage = 'Checkout' AND cj.`Action` = 'Purchase' THEN 1 ELSE 0 END) AS Total_Purchases,
        ROUND(
            (SUM(CASE WHEN cj.Stage = 'Checkout' AND cj.`Action` = 'Purchase' THEN 1 ELSE 0 END) * 100.0) / COUNT(DISTINCT cj.CustomerID), 2
        ) AS Conversion_Rate
    FROM customer_journey cj
    JOIN customers c ON cj.CustomerID = c.CustomerID
    GROUP BY c.Gender
),

customer_conversion_country AS (
    SELECT 
        g.Country,
        COUNT(DISTINCT cj.CustomerID) AS Total_Customers,
        SUM(CASE WHEN cj.Stage = 'Checkout' AND cj.`Action` = 'Purchase' THEN 1 ELSE 0 END) AS Total_Purchases,
        ROUND(
            (SUM(CASE WHEN cj.Stage = 'Checkout' AND cj.`Action` = 'Purchase' THEN 1 ELSE 0 END) * 100.0) / COUNT(DISTINCT cj.CustomerID), 2
        ) AS Conversion_Rate
    FROM customer_journey cj
    JOIN customers c ON cj.CustomerID = c.CustomerID
    JOIN geography g ON c.GeographyID = g.GeographyID
    GROUP BY g.Country
)

-- Corrected UNION statement
SELECT 'Age Range' AS Group_By, Age_Range AS Group_Value, Total_Customers, Total_Purchases, Conversion_Rate FROM customer_conversion_age
UNION ALL
SELECT 'Gender' AS Group_By, Gender AS Group_Value, Total_Customers, Total_Purchases, Conversion_Rate FROM customer_conversion_gender
UNION ALL
SELECT 'Country' AS Group_By, Country AS Group_Value, Total_Customers, Total_Purchases, Conversion_Rate FROM customer_conversion_country;











