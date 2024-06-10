from pyspark.sql import SparkSession

def task_1_1(input_paths):
   
    spark = SparkSession.builder.appName("Task1.1").getOrCreate()

    
    cases_2012 = spark.read.csv(input_paths[0], header=True, inferSchema=True)
    cases_2013 = spark.read.csv(input_paths[1], header=True, inferSchema=True)
    cases_2014 = spark.read.csv(input_paths[2], header=True, inferSchema=True)

    
    all_cases = cases_2012.union(cases_2013).union(cases_2014)

    
    state_key = spark.read.csv(input_paths[3], header=True, inferSchema=True)

    
    cases_with_state = all_cases.join(state_key, ["year", "state_code"], "left")

    
    state_crime_counts = cases_with_state.groupBy("state_name").count()

    
    top_10_states = state_crime_counts.orderBy("count", ascending=False).limit(10)

    
    top_10_states_list = top_10_states.select("state_name").rdd.flatMap(lambda x: x).collect()


	
    

    

    
    judges = spark.read.csv(input_paths[6], header=True, inferSchema=True)       # acts_sections.csv 

   
    judge_case_mapping = spark.read.csv(input_paths[5], header=True, inferSchema=True)  # judge_case_merge_key.csv
    
    
    
    intermediate_join  = all_cases.join(judges, ["ddl_case_id"])
    final_join = intermediate_join.join(judge_case_mapping,["ddl_case_id"])
    
    filtered_cases = final_join[final_join['criminal'] == 1]
    
    filtered_cases_2 = filtered_cases.filter(filtered_cases['ddl_decision_judge_id'].isNotNull())

    
    
    judge_case_counts = filtered_cases_2.groupBy("ddl_decision_judge_id").count()

    
    most_active_judge = judge_case_counts.orderBy("count", ascending=False).first()

    
    judge_with_most_cases = most_active_judge["ddl_decision_judge_id"]
    
    output = (top_10_states_list,judge_with_most_cases)

    return output
    
    

if __name__ == "__main__":
    import sys 
    if len(sys.argv) != 9:
        print("Usage: spark-solution.py <input_paths> <output_path>")
        sys.exit(1)

    input_paths = sys.argv[1:8]
    output_path = sys.argv[8]

    
    output = task_1_1(input_paths)

    
    try:
        with open(output_path, "w") as output_file:
            output_str = str(output)
            output_file.write(output_str)
    except Exception as e:
        print("Error writing the output file:",str(e))
