import jaccardBase


def buildMapping(tracks, link = 'single'):
	if link == 'single':
		return singleLink(tracks)
	elif link == 'average':
		return averageLink(tracks)
	elif link == 'complete'
		return completeLink(tracks)


'''Build clusters considering just one song equal.'''
def singleLink(tracks):
	song_to_cluster_dic = {}
	cluster_to_songs_dic = {}
	
	#For eah song we check if it can belong to an existing cluster 
	for song in tracks:	
		clustered = False		
		for cluster, tracks in cluster_to_songs_dic.items():
			for t in tracks:
				#If one song in a cluster matches, we add the song to that cluster
				if classify(song, t):
					tracks.append(song)
					#Add also the mapping song->cluster
					song_to_cluster_dic[song] = cluster
					clustered = True
					break;			
			if clustered: break;
		
		#If we did not find a cluster, create one.
		if not clustered:
			n = len(cluster_to_songs_dic)
			cluster_to_songs_dic[n+1] = [song]
			song_to_cluster_dic[song] = n+1

	return song_to_cluster_dic, cluster_to_songs_dic




'''Build clusters where all the tracks are equal'''
def completeLink(tracks):
	song_to_cluster_dic = {}
	cluster_to_songs_dic = {}
	
	#For eah song we check if it can belong to an existing cluster 
	for song in tracks:
		clustered = False	
		for cluster, tracks in cluster_to_songs_dic.items():
			broken = False
			for t in tracks:
				#If there is at least one song that do NOT match, break.
				if not classify(song, t):
					broken = True
					break;
			#If every tracks gave result of equality		
			if not broken:
				tracks.append(song)
				song_to_cluster_dic[song] = cluster
				clustered = True
				break;
		
		#If we did not find a cluster, create one.
		if not clustered:
			n = len(cluster_to_songs_dic)
			cluster_to_songs_dic[n+1] = [song]
			song_to_cluster_dic[song] = n+1

	return song_to_cluster_dic, cluster_to_songs_dic