def normalize_dataset(dataset, minmax):
    for row in dataset:
        for i in range(len(row)):
            row[i] = (row[i] - minmax[i][0]) / (minmax[i][1] - minmax[i][0])

def main():
    dataset = []
    n_folds = 2
    l_rate = 0.001
    n_epoch = 1000
    scores = evaluate_algorithm(dataset, logistic_regression, n_folds, l_rate, n_epoch)

main()