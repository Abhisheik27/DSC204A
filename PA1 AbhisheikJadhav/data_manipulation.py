import ray, json, time
ray.shutdown()
ray.init(num_cpus=2)
import modin.pandas as pd


def run_task2(path):
    raw_df = pd.read_csv(path)

    ## PLEASE COMPLETE THIS: START 
    data = data.drop(columns=['Unnamed: 0.1', 'Unnamed: 0'])

    #replacing NaN values by 0
    data['vote'].fillna(0, inplace = True) 

    #converting data in vote columns to string
    data['vote'] = data['vote'].astype(str)

    #replacing ',' in vote, for example we have: 1,000 so converting that to 1000
    data['vote'] = data['vote'].str.replace(',','').astype(float).astype(int)

    #using column unix review time to get a date time dtype column 'review time'
    data['review time'] = pd.to_datetime(data['unixReviewTime'], unit = 's')

    #extracting years from review time
    data['reviewYear'] = data['review time'].dt.year

    #Dropping waste columns
    data = data.drop(columns = ['unixReviewTime', 'reviewTime', 'review time'])

    #Count the number of products reviewed by each reviewer
    num_products_reviewed = data.groupby('reviewerID')['overall'].count().rename('num_product_reviewed')

    #Calculate the average rating given by each reviewer
    mean_rating = data.groupby('reviewerID')['overall'].mean().rename('mean_rating')

    #Find the latest year each reviewer has given a review
    latest_review_year = data.groupby('reviewerID')['reviewYear'].max().rename('latest_review_year')
    #Sum the total number of helpful votes each reviewer has received
    num_helpful_votes = data.groupby('reviewerID')['vote'].sum().rename('num_helpful_votes')

    #Combine all the computed series into a single dataframe
    result_data = pd.concat([num_products_reviewed, mean_rating, latest_review_year, num_helpful_votes], axis=1).reset_index()


    ## PLEASE COMPLETE THIS: END
    submit = result_data.describe().round(2)
    with open('results_PA0.json', 'w') as outfile: json.dump(json.loads(submit.to_json()), outfile)

if __name__ == "__main__":
    raw_dataset_path = "public/modin_dev_dataset.csv"  # PLEASE SET THIS
    a = time.time()
    run_task2(raw_dataset_path)
    b = time.time()
    print(b-a)