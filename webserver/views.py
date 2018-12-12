from django.shortcuts import render


def about(request, *args, **kwargs):
    '''
    关于函数视图
    '''
    return render(request, 'about.html', {})

