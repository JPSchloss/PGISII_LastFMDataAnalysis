# Jonathan Schlosser
# INLS 570
# Final Version of Project 2
# April 24, 2020

# This is a simple data exploration of last.fm streaming data. This is a basic approach
# and is not a full exploratory data analysis nor a hyper-detailed approach. Additionally,
# the approach here uses pandas and numpy, as well as some base methods.


# Loading in the necessary libraries
import pandas as pd
import numpy as np

# Adjusting Pandas Settings to display more of the table.
desired_width = 320
pd.set_option('display.width', desired_width)
np.set_printoptions(linewidth=desired_width)
pd.set_option('display.max_columns', 10)

# Setting the format for float numbers
pd.options.display.float_format = '{:.0f}'.format

# Loading in the Data... adding an exception handler in case there is a problem loading the file.
try:
    artists_df = pd.read_csv('hetrec2011-lastfm-2k/artists.dat', encoding="utf-8", sep="\t")
    user_artists_df = pd.read_csv('hetrec2011-lastfm-2k/user_artists.dat',
                                  encoding="utf-8", sep="\t",
                                  index_col=['userID', 'artistID'])
    user_friends_df = pd.read_csv('hetrec2011-lastfm-2k/user_friends.dat',
                                  encoding="utf-8",
                                  sep="\t")

    # Code to bring in the user_taggedartists file but didnt seem to need it for the project.
    #user_taggedartists_df = pd.read_csv('*/hetrec2011-lastfm-2k/user_taggedartists.dat', encoding = "utf-8", sep = "\t")

except FileNotFoundError:
    print('There was an error loading the data. Please try again.')
    quit()

print('\033[1m' + 'last.fm Basic Exploratory Data Analysis' + '\033[0m')

print("----------------------------------------", "\n", sep="")

# Initial Check of the Data - checking basic statistics...
print('\033[1m' + "Initial Probe Into the Data:" + '\033[0m')
print("\tNumber of User IDs in Dataset:", len(np.unique(user_friends_df["userID"])))
print("\tNumber of Artists in Dataset:", len(artists_df))
print("\tNumber of User-Artist Connections in Dataset:", len(user_artists_df))
print("\tNumber of Friend Connections in Dataset:", len(user_friends_df))


# Who are the top artists in terms of play counts?
    # Summing the play counts for each user.
    # Sorting those values and selecting the top ten.
    # Merging with artist name and other information.
    # Setting the name as the index to make output neater.
    # Printing the output.

print("\n", "========================================", "\n", sep="")
print('\033[1m' + 'Top 10 Artists By Play Count' + '\033[0m'+ '\n')

play_count_all = user_artists_df.sum(level=1)
play_count = play_count_all.sort_values(by='weight', ascending=False)[0:10]
working_file = play_count.merge(artists_df, left_index=True, right_on='id')
working_file = working_file.rename(columns={'id': 'Artist ID', 'weight': 'Play Count', 'name': 'Artist Name'})
working_file = working_file.set_index('Artist Name')
print(working_file[['Artist ID', 'Play Count']])


# What artists have the most listeners?
    # Counting the the users associated withe ach artist.
    # Sorting those values and selecting the top ten.
    # Merging with artist name and other information.
    # Setting the name as the index to make output neater.
    # Printing the output.

print("\n", "========================================", "\n", sep="")
print('\033[1m' + "Top 10 Artists By User Count" + '\033[0m' + '\n')

listener_count_all = user_artists_df.count(level=1)
listener_count = listener_count_all.sort_values(by='weight', ascending=False)[0:10]
working_file = listener_count.merge(artists_df, left_index=True, right_on='id')
working_file = working_file.rename(columns={'id': 'Artist ID', 'weight': 'User Count', 'name': 'Artist Name'})
working_file = working_file.set_index('Artist Name')
print(working_file[['Artist ID', 'User Count']])

# Who are the top users in terms of play counts?
    # Summing the number of play counts for each user.
    # Sorting those values and selecting the top ten.
    # Printing the output.

print("\n", "========================================", "\n", sep="")
print('\033[1m' + "Top 10 Users by Play Count" + '\033[0m' + '\n')

user_count_all = user_artists_df.sum(level=0)
user_counts = user_count_all.sort_values(by='weight', ascending=False)[0:10]
user_counts = user_counts.rename(columns={'weight': 'Play Count'})
user_counts = user_counts.rename_axis('User ID')
print(user_counts)


# What artists have the highest average number of plays per listener?
    # Dividing the total play count by the number of listeners to identify the average per listener.
    # Merging with artist name and other information.
    # Merging with the listener and play counts.
    # Renaming columns to ensure that analysis stays accurate (removing repeated 'weight' columns)
    # Setting the name as the index to make output neater.
    # Sorting values by the average and selecting the top ten.
    # Printing the output in a neat manner.

print("\n", "========================================", "\n", sep="")
print('\033[1m' + "Top 10 Artists By Average Plays Per Listener" + '\033[0m' + '\n')

average_count_all = play_count_all/listener_count_all
working_file = play_count_all.merge(artists_df, left_index=True, right_on='id')
working_file = listener_count_all.merge(working_file, left_index=True, right_on='id')
working_file = average_count_all.merge(working_file, left_index=True, right_on='id')
working_file.rename(columns={'name': 'Artist Name', 'id': 'Artist ID', 'weight': 'Average Plays',
                             'weight_x': 'Listeners', 'weight_y': 'Play Counts'}, inplace=True)
working_file = working_file.set_index('Artist Name')
avg_work_file = working_file.sort_values(by='Average Plays', ascending=False)[0:10]
print(avg_work_file[['Artist ID', 'Play Counts', 'Listeners', 'Average Plays']])


# What artists with at least 50 listeners have the highest average number of plays per listener?
    # Identifying users with more than 50 friends from the dataframe created above.
    # Sorting the dataframe by the average and selecting the top 10.
    # Printing out the results.

print("\n", "========================================", "\n", sep="")
print('\033[1m' + "Top 10 Artists By Average Plays Per Listener With At Least 50 Listeners" + '\033[0m' + '\n')

avg_work_file_50 = working_file[working_file['Listeners'] >= 50]
avg_work_file_50 = avg_work_file_50.sort_values(by='Average Plays', ascending=False)[0:10]
print(avg_work_file_50[['Artist ID', 'Play Counts', 'Listeners', 'Average Plays']])


# Do users with five or more friends listen to more songs?
    # Adding a column to the user_friends_df dataframe to establish a column to work with.
    # Creating a hierarchical index to help with counting.
    # Counting the friends for each user.
    # Selecting users with less than 5 and 5 or more and putting them into respective dataframes.
    # Calculating the average plays for each user.
    # Printing out the results.

print("\n", "========================================", "\n", sep="")
print('\033[1m' + "Average Song Plays" + '\033[0m' + '\n')

user_friends_df['count'] = 1
friend_count = user_friends_df.set_index(['userID', 'friendID'])
friend_count = friend_count.count(level=0)

friend_count_M_5 = friend_count[friend_count['count'] >= 5]
friend_count_M_5 = friend_count_M_5.merge(user_count_all, left_index=True, right_index=True)
above_5_total = friend_count_M_5['weight'].sum()
above_5_average = int(above_5_total/len(friend_count_M_5))
print('Average song plays for users with 5 or more friends:\t', above_5_average)


friend_count_L_5 = friend_count[friend_count['count'] < 5]
friend_count_L_5 = friend_count_L_5.merge(user_count_all, left_index=True, right_index=True)
below_5_total = friend_count_L_5['weight'].sum()
below_5_average = int(below_5_total/len(friend_count_L_5))
print('Average song plays for users with less than 5 friends:\t', below_5_average)


# Jaccard Index of Similar Artists by Listeners
    # Creating a working file to compare listeners for each artist.
    # Getting the name of the two artists to help with output.
    # Establishing sets for each of the two artists' listeners.
    # Calculating the intersection and the union of the two sets.
    # Dividing the above to get the Jaccard Similarity Index
    # Setting up a nice output.
    # Calling the function for the examples in the the assignment.

print("\n", "========================================", "\n", sep="")
print('\033[1m' + "Jaccard Index Examples" + '\033[0m')

work_user_artists = user_artists_df.reset_index()
jaccard_work_file = work_user_artists.merge(artists_df[['id', 'name']], left_on='artistID', right_on='id')
jaccard_work_file = jaccard_work_file.set_index('id')


def artist_sim(aid1, aid2):

    artist_1_name = jaccard_work_file.loc[aid1].name.iloc[1]
    artist_2_name = jaccard_work_file.loc[aid2].name.iloc[1]

    artist_1 = set(jaccard_work_file[jaccard_work_file['artistID'] == aid1].userID)
    artist_2 = set(jaccard_work_file[jaccard_work_file['artistID'] == aid2].userID)

    inter = len(artist_1.intersection(artist_2))
    artist_union = len(artist_1.union(artist_2))

    jaccard = inter/artist_union

    print('\nJaccard Index Results:')
    print('\nArtist 1\t=\t', artist_1_name, ' (', aid1, ')',
          '\nArtist 2\t=\t', artist_2_name, ' (', aid2, ')',
          '\nJaccard \t=\t', "{:.4f}".format(jaccard), sep='')


artist_sim(735, 562)
artist_sim(735,89)
artist_sim(735,289)
artist_sim(89,289)
artist_sim(89,67)
artist_sim(67,735)

#==== END OF CODE ====#