from django.shortcuts import redirect, render
from django.http import Http404
from django.http import HttpResponseRedirect
from django.http import HttpResponse
from django.contrib import messages
import praw
import pandas as pd
import numpy as np

from u_nique_recs.settings import BASE_DIR


from .forms import UsernameForm

import requests
from collections import Counter

def home(request):
    if request.method == "POST":
        form = UsernameForm(request.POST)
        if form.is_valid():
            user = form.cleaned_data['user_name']

            slash_idx = user.rfind('/')
            if slash_idx != -1:
                user = user[slash_idx + 1:]

            return redirect('user_reccs', user=user)
    else:
        form = UsernameForm()
    return render(request, 'home.html', {'form': form})

def user_reccs(request, user):
    try:
        reddit = praw.Reddit(
            client_id="",
            client_secret="",
            user_agent="Unique Reccomendations Program For Reddit By /u/Zeeman12",
        )
        sub_list = []
        for comment in reddit.redditor(user).comments.new(limit=500):
            sub_list.append(str(comment.subreddit))

        sub_list = dict(Counter(sub_list))
    except:
        messages.info(request, 'Invalid Username')
        return redirect('home')
    
    if len(sub_list) < 1:
        messages.warning(request, 'User has no comments')
        return redirect('home')
    
    
    subs = get_recs(BASE_DIR / 'traindf2k.pkl', sub_list)
    
    sub_posts = []
    for idx,s in enumerate(subs):
        sub_posts.append([idx])
        sub_posts[idx].append(s)
        sub_posts[idx].append(reddit.subreddit(s).icon_img)
        sub_posts[idx].append([])
        for submission in reddit.subreddit(s).hot(limit=5):
            sub_posts[idx][3].append([submission.title,submission.permalink])

    return render(request, 'user_reccs.html', {'user' : user, 'sub_posts' : sub_posts, 'subs' : list(sub_list.items())})




def get_recs(filepath_df,new_user):
    #user_id is
    

    #Initialize df 
    df = pd.read_pickle(filepath_df)
    df= df.append(new_user,ignore_index=True)
    df.fillna(0)
    subreddits = df.columns
    mat = df.to_numpy()
    mat[np.isnan(mat)] = 0
    #mat = (mat > 0).astype(float)

    user_id = len(mat) - 1

    np.seterr(divide='ignore', invalid='ignore')

    user_train_like = []
    for u in range(len(mat)):
        user_train_like.append(np.where(mat[u, :] > 0)[0])

    numer = np.matmul(mat, mat.T)
    denom = np.sum(mat ** 2, axis=1, keepdims=True) ** 0.5
    Cosine = numer / np.matmul(denom, denom.T)

    recommendation = []
    for u in range(len(mat)):
        similarities = Cosine[u, :]
        similarities[u] = -1
        N_idx = np.argpartition(similarities, -10)[-10:]
        N_sim = similarities[N_idx]
        scores = np.sum(N_sim.reshape((-1, 1)) * mat[N_idx, :], axis=0) / np.sum(N_sim)

        train_like = user_train_like[u]
        scores[train_like] = -9999
        top50_iid = np.argpartition(scores, -5)[-5:]
        top50_iid = top50_iid[np.argsort(scores[top50_iid])[-1::-1]]
        recommendation.append(top50_iid)

    recommendation = np.array(recommendation)


    subs = np.nonzero(mat[user_id])
    already_subscribed = []
    recommendations = []
    for sub in subs:
        already_subscribed.append(subreddits[sub])

    for rec in recommendation[user_id]:
        recommendations.append(subreddits[rec])

    return recommendations
