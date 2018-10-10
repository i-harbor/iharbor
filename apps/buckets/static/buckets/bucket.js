;(function () {

    //
    // 全选/全不选
    //
    $(':checkbox[data-check-target]').click(function () {
        let target = $(this).attr('data-check-target');
        if ($(this).prop('checked')) {
            $(target).prop('checked', true); // 全选
            $(target).parents('tr').addClass('danger'); // 选中时添加 背景色类
        } else {
            $(target).prop('checked', false); // 全不选
            $(target).parents('tr').removeClass('danger');// 不选中时移除 背景色类
        }
    });


    //
    // 表格中每一行单选checkbox
    //
    $('.item-checkbox').click(function () {
        if ($(this).prop('checked')) {
            $(this).parents('tr').addClass('danger');
        } else {
            $(this).parents('tr').removeClass('danger');
        }
    })


    //
    // 进度条设置
    //
    function setProgressBar(obj_bar, width, hide=false){
        width = Math.floor(width);
        var $bar = $(obj_bar);
        $bar.children().attr({"style": "min-width: 2em;width: " + width + "%;"});
        if (hide)
            $bar.hide();
        else
            $bar.show();
    }

    //
    // 上传文件按钮
    //
    $("#btn-upload-file").on("click",
        function () {
            $("#div-upload-file").show();
        }
    );

    //
    // 取消上传文件按钮
    //
    $("#btn-cancel").on("click", function () {
        $("#div-upload-file>form>:file").val('');
        $("#div-upload-file").hide();
    })


    // function toByteArray(arr) {
    //     var n = arr.length;
    //     if (typeof(n) === 'undefined') {
    //         // if IE 10+, arr is ArrayBuffer
    //         n = arr.byteLength;
    //         return new Uint8Array(arr, 0, n);
    //     }
    //     var b = new Uint8Array(n);
    //     for (var i = 0; i < n; i++) {
    //         b[i] = arr[i].charCodeAt(0);
    //     }
    //     arr = null;
    //     return b;
    // }


    function onUploadFile(e) {
        e.preventDefault();
        let $form = $("#div-upload-file>form").first();
        let url = $form.attr('ajax_upload_url');
        if (!url) {
            alert('获取文件上传url失败，请刷新网页后重试');
            return;
        }
        let $file = $form.children(":file");
        if (!$file.val()) {
            alert('请先选择上传的文件');
            return;
        }
        let pathname = window.location.pathname;
        let bucket_name = '';
        let dir_path = '';
        if (pathname) {
            pathname = pathname.replace(/(^\/)|(\/$)/g, '');//删除前后的/
            let l = pathname.split('/');
            bucket_name = l[1];
            l.splice(0, 2);
            dir_path = l.join('/');
        } else {
            alert('当前url有误');
        }
        let csrfmiddlewaretoken = $("[name='csrfmiddlewaretoken']").first().val();
        uploadFileCreate(url, bucket_name, dir_path, $file[0].files[0], csrfmiddlewaretoken);
    }


    $('#upload_submit').on('click', onUploadFile);


    //
    // 创建文件对象
    //
    function uploadFileCreate(url, bucket_name, dir_path, file, csrf_code, overwrite = false, file_md5 = '') {
        var formData = new FormData();
        formData.append("bucket_name", bucket_name);
        formData.append("dir_path", dir_path);
        formData.append("file_name", file.name);
        formData.append("file_size", file.size);
        formData.append("file_md5", file_md5);
        formData.append("overwrite", true);
        formData.append("csrfmiddlewaretoken", csrf_code);

        $.ajax({
            url: url,
            type: "POST",
            data: formData, //必须false才会自动加上正确的Content-Type
            contentType: false,
            processData: false,//必须false才会避开jQuery对 formdata 的默认处理,XMLHttpRequest会对 formdata 进行正确的处理
            success: function (data) {
                console.log(data);
                if (data.id) {
                    let put_url = url + data.id + '/';
                    uploadFile(put_url, bucket_name, dir_path, file, csrf_code);
                } else {
                    alert('创建文件对象失败');
                }
            },
            error: function (err) {
                console.log(err);
                alert('创建文件对象失败');
            }
        });
    }

    //
    //文件上传
    //
    function uploadFile(put_url, bucket_name, dir_path, file, csrf_code, offset = 0) {
        let file_size = file.size;
        let chunk_size = 2 * 1024 * 1024;//2MB
        let start = offset;
        let end = start;
        let res;
        let $rogressbar = $("#upload-progress-bar");
        for (; ;) {
            if (start < file_size) {
                if ((start + chunk_size) > file_size) {
                    end = file_size;
                } else {
                    end = start + chunk_size;
                }
            } else if (start >= file_size) {
                alert('文件上传完成');
                break;
            }
            var chunk = file.slice(start, end);
            console.log(chunk);
            res = uploadFileChunk(put_url, bucket_name, offset = start, chunk = chunk, chunk_size = chunk.size);
            console.log(res);
            //文件块上传失败
            if (!res.ok) {
                alert('文件上传发生错误');
                break;
            }
            start = end;
            //进度条
            var percent = 100 * end / file_size;
            if (percent > 100) {
                percent = 100;
            }
            setProgressBar($rogressbar, percent, true);
        }
    }

    //
    //上传一个文件数据块
    //
    function uploadFileChunk(url, bucket_name, offset, chunk, chunk_size) {
        var formData = new FormData();
        formData.append("bucket_name", bucket_name);
        formData.append("chunk_offset", offset);
        formData.append("chunk", chunk);
        formData.append("chunk_size", chunk_size);
        var res = null;
        $.ajax({
            url: url,
            type: "PUT",
            data: formData,
            contentType: false,//必须false才会自动加上正确的Content-Type
            processData: false,//必须false才会避开jQuery对 formdata 的默认处理,XMLHttpRequest会对 formdata 进行正确的处理
            beforeSend: function (xhr, settings) {//set csrf cookie
                var csrftoken = getCookie('csrftoken');
                if (!csrfSafeMethod(settings.type) && !this.crossDomain) {
                    xhr.setRequestHeader("X-CSRFToken", csrftoken);
                }
            },
            success: function (data) {
                res = {'ok': true, 'data': data};
            },
            error: function (err) {
                res = {'ok': false, 'data': err};
            },
            async: false
        });
        return res;
    }

    function getCsrfMiddlewareToken() {
        return $("[name='csrfmiddlewaretoken']").first().val();
    }
}());

function getCookie(name) {
    var cookieValue = null;
    if (document.cookie && document.cookie != '') {
        var cookies = document.cookie.split(';');
        for (var i = 0; i < cookies.length; i++) {
            var cookie = jQuery.trim(cookies[i]);
            if (cookie.substring(0, name.length + 1) == (name + '=')) {
                cookieValue = decodeURIComponent(cookie.substring(name.length + 1));
                break;
            }
        }
    }
    return cookieValue;
}

function csrfSafeMethod(method) {
    // these HTTP methods do not require CSRF protection
    return (/^(GET|HEAD|OPTIONS|TRACE)$/.test(method));
}


