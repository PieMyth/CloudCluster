#!/usr/bin/env python3

import matplotlib.pyplot as plt  # visual plotting of data
import seaborn as sns            # visual plotting of data
import pandas as pd              # handling data
import config as CFG             # configuration file
import os


def barplot(filename: str, x_label: str, y_label: str, img_title: str, pairs: [str]):
    """
    Graph barplot from resulting benchmarking measuremnts
    :param filename: file containing data
    :param x_label: x label's name
    :param y_label: y label's name
    :param img_title: title of saved file
    :param pairs: color combination for bars
    """
    current_dir = os.getcwd()
    file_path = current_dir + '\\' + filename
    image_path = current_dir + '\\' + img_title

    sns.set(style='whitegrid')
    df = pd.read_csv(file_path)
    sns.set_palette(sns.color_palette(pairs))
    ax = sns.catplot(x='query',
                     y='benchmark_ms',
                     hue='Platforms',
                     kind='bar',
                     data=df,
                     legend=True,
                     legend_out=True)

    ax.set(xlabel=x_label, ylabel=y_label)

    # Save plot image
    plt.savefig(image_path)

    # Display plot
    plt.show()


if __name__ == '__main__':
    for data in CFG.DATA_FILES:
        barplot(data + '.csv', CFG.X_AXIS, CFG.Y_AXIS, data + '.png', CFG.COLORS)
    print('\nDone!')
