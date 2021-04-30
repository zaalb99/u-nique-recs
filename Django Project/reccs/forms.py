from django import forms

class UsernameForm(forms.Form):
    user_name = forms.CharField(label=False, max_length=100,
                                widget=forms.TextInput(attrs={'placeholder': 'Username or /u/Username',
                                                              'class': 'form-control'}))
