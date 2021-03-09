
from random import seed
from random import randrange
from csv import reader
from math import exp

def main():
    seed(1)
    filename = 'pima-indians-diabetes.csv'
    load_csv_dataset = list()
    with open(filename, 'r') as load_csv_file:
        load_csv_csv_reader = reader(load_csv_file)
        for load_csv_row in load_csv_csv_reader:
            if (not load_csv_row):
                continue
            load_csv_dataset.append(load_csv_row)
    virtual_return_register_0 = load_csv_dataset
    dataset = virtual_return_register_0
    for i in range(len(dataset[0])):
        for str_column_to_float_row in dataset:
            str_column_to_float_row[i] = float(str_column_to_float_row[i].strip())
    dataset_minmax_minmax = list()
    for dataset_minmax_i in range(len(dataset[0])):
        dataset_minmax_col_values = [dataset_minmax_row[dataset_minmax_i] for dataset_minmax_row in dataset]
        dataset_minmax_value_min = min(dataset_minmax_col_values)
        dataset_minmax_value_max = max(dataset_minmax_col_values)
        dataset_minmax_minmax.append([dataset_minmax_value_min, dataset_minmax_value_max])
    virtual_return_register_1 = dataset_minmax_minmax
    minmax = virtual_return_register_1
    for normalize_dataset_row in dataset:
        for normalize_dataset_i in range(len(normalize_dataset_row)):
            normalize_dataset_row[normalize_dataset_i] = ((normalize_dataset_row[normalize_dataset_i] - minmax[normalize_dataset_i][0]) / (minmax[normalize_dataset_i][1] - minmax[normalize_dataset_i][0]))
    n_folds = 5
    l_rate = 0.1
    n_epoch = 100
    cross_validation_split_dataset_split = list()
    cross_validation_split_dataset_copy = list(dataset)
    cross_validation_split_fold_size = int((len(dataset) / n_folds))
    for cross_validation_split_i in range(n_folds):
        cross_validation_split_fold = list()
        while (len(cross_validation_split_fold) < cross_validation_split_fold_size):
            cross_validation_split_index = randrange(len(cross_validation_split_dataset_copy))
            cross_validation_split_fold.append(cross_validation_split_dataset_copy.pop(cross_validation_split_index))
        cross_validation_split_dataset_split.append(cross_validation_split_fold)
    virtual_return_register_2 = cross_validation_split_dataset_split
    evaluate_algorithm_folds = virtual_return_register_2
    evaluate_algorithm_scores = list()
    for evaluate_algorithm_fold in evaluate_algorithm_folds:
        evaluate_algorithm_train_set = list(evaluate_algorithm_folds)
        evaluate_algorithm_train_set.remove(evaluate_algorithm_fold)
        evaluate_algorithm_train_set = sum(evaluate_algorithm_train_set, [])
        evaluate_algorithm_test_set = list()
        for evaluate_algorithm_row in evaluate_algorithm_fold:
            evaluate_algorithm_row_copy = list(evaluate_algorithm_row)
            evaluate_algorithm_test_set.append(evaluate_algorithm_row_copy)
            evaluate_algorithm_row_copy[(- 1)] = None
        logistic_regression_predictions = list()
        coefficients_sgd_coef = [0.0 for coefficients_sgd_i in range(len(evaluate_algorithm_train_set[0]))]
        for coefficients_sgd_epoch in range(n_epoch):
            for coefficients_sgd_row in evaluate_algorithm_train_set:
                predict_yhat = coefficients_sgd_coef[0]
                for predict_i in range((len(coefficients_sgd_row) - 1)):
                    predict_yhat += (coefficients_sgd_coef[(predict_i + 1)] * coefficients_sgd_row[predict_i])
                virtual_return_register_3 = (1.0 / (1.0 + exp((- predict_yhat))))
                coefficients_sgd_yhat = virtual_return_register_3
                coefficients_sgd_error = (coefficients_sgd_row[(- 1)] - coefficients_sgd_yhat)
                coefficients_sgd_coef[0] = (coefficients_sgd_coef[0] + (((l_rate * coefficients_sgd_error) * coefficients_sgd_yhat) * (1.0 - coefficients_sgd_yhat)))
                for coefficients_sgd_i in range((len(coefficients_sgd_row) - 1)):
                    coefficients_sgd_coef[(coefficients_sgd_i + 1)] = (coefficients_sgd_coef[(coefficients_sgd_i + 1)] + ((((l_rate * coefficients_sgd_error) * coefficients_sgd_yhat) * (1.0 - coefficients_sgd_yhat)) * coefficients_sgd_row[coefficients_sgd_i]))
        virtual_return_register_4 = coefficients_sgd_coef
        logistic_regression_coef = virtual_return_register_4
        for logistic_regression_row in evaluate_algorithm_test_set:
            predict_yhat = logistic_regression_coef[0]
            for predict_i in range((len(logistic_regression_row) - 1)):
                predict_yhat += (logistic_regression_coef[(predict_i + 1)] * logistic_regression_row[predict_i])
            virtual_return_register_5 = (1.0 / (1.0 + exp((- predict_yhat))))
            logistic_regression_yhat = virtual_return_register_5
            logistic_regression_yhat = round(logistic_regression_yhat)
            logistic_regression_predictions.append(logistic_regression_yhat)
        virtual_return_register_6 = logistic_regression_predictions
        evaluate_algorithm_predicted = virtual_return_register_6
        evaluate_algorithm_actual = [evaluate_algorithm_row[(- 1)] for evaluate_algorithm_row in evaluate_algorithm_fold]
        accuracy_metric_correct = 0
        for accuracy_metric_i in range(len(evaluate_algorithm_actual)):
            if (evaluate_algorithm_actual[accuracy_metric_i] == evaluate_algorithm_predicted[accuracy_metric_i]):
                accuracy_metric_correct += 1
        virtual_return_register_7 = ((accuracy_metric_correct / float(len(evaluate_algorithm_actual))) * 100.0)
        evaluate_algorithm_accuracy = virtual_return_register_7
        evaluate_algorithm_scores.append(evaluate_algorithm_accuracy)
    virtual_return_register_8 = evaluate_algorithm_scores
    scores = virtual_return_register_8
    print(('Scores: %s' % scores))
    print(('Mean Accuracy: %.3f%%' % (sum(scores) / float(len(scores)))))

main()