# DSC204A
Includes my work done(Assignments) in the DSC204A course: Scalable Data Systems

## Programming Assignment 3:
The aim of Programming Assignment 3 was to train and tune a machine-learning model using Ray. The assignment consisted of two main tasks: feature engineering with Modin on Ray (Task 1) and training and tuning with Ray (Task 2).

**Task 1: Feature Engineering with Modin on Ray**

In Task 1, I performed feature engineering on the Amazon Reviews dataset using Modin on Ray. This involved several subtasks:

* Flatten categories and salesRank: I extracted the most general category from the nested list of categories and flattened the salesRank column into two separate columns, bestSalesCategory and bestSalesRank, handling null values appropriately.

* Flatten related: I calculated the mean price and length of the "also viewed" attribute array, handling null and dangling references gracefully.

* Impute price: I imputed null values in the price column using both mean and median imputation techniques, and imputed null and empty strings in the title column with a special string 'unknown'.

* Process title and one-hot encode category: I processed the title column by converting it to lowercase and splitting it into an array of strings. Additionally, I performed one-hot encoding on the category column.

**Task 2: Training and Tuning with Ray**

In Task 2, I trained a machine learning model using the preprocessed data and tuned its hyperparameters using Ray Tune. This task involved:

* Distributed Xgboost with Ray Train: I trained an Xgboost model to predict user ratings for products, minimizing mean squared error. The model was trained with specified hyperparameters and a scaling configuration for Ray.

* Tuning with Ray Tune: I performed a grid search for hyperparameters using Ray Tune, selecting the best model based on the lowest validation RMSE. The hyperparameters tuned Ire max depth, eta, and subsample, with a limited grid of values.

**Conclusion:**
I developed the solutions for this assignment in Jupyter notebooks provided on DataHub, using the Ray environment with specified CPU and RAM configurations. Package installations were already handled, and caution was advised against installing additional libraries that could persist across cluster restarts.
Overall, this assignment provided valuable hands-on experience in feature engineering, model training, and hyperparameter tuning using Ray, contributing to a deeper understanding of scalable data systems and machine learning workflows.
