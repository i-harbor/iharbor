;(function() {
    //
    // 创建新存储桶表单异步提交
    //
    $form_new_bucket = $('#form-new-bucket');
    $form_new_bucket.on('submit', function (event) {
        event.preventDefault();
        $.ajax({
            url: $form_new_bucket.attr('action'),
            type: 'post',
            data: $form_new_bucket.serialize(),
            success: function (data) {
                if (data.code === 200)
                    location.href = data.redirect_to;//创建成功跳转
                else
                    $('#tip_text').text(data.error_text);
            },
            error: function (err) {
                $('#tip_text').text(err.status+':'+err.statusText);
            },
            headers: {'X-Requested-With': 'XMLHttpRequest'},//django判断是否是异步请求时需要此响应头
            clearForm: false,//禁止清除表单
            resetForm: false //禁止重置表单
        });
    });

    //
    //form 表单获取所有数据 封装方法
    //
    function getFormJson(form_node) {
        var o = {};
        var a = $(form_node).serializeArray();
        $.each(a, function () {
            if (o[this.name] !== undefined) {
                if (!o[this.name].push) {
                    o[this.name] = [o[this.name]];
                }
                o[this.name].push(this.value || '');
            } else {
                o[this.name] = this.value || '';
            }
        });

        return o;
    }

    //
    // 创建新的存储桶点击事件处理（对话框方式）
    //
    function on_create_bucket(){
        swal({
            title: '请输入一个新的存储桶名称',
            input: 'text',
            inputAttributes: {
                autocapitalize: 'off'
            },
            showCancelButton: true,
            confirmButtonText: '创建',
            showLoaderOnConfirm: true,
            preConfirm: (input_name) => {
                let url = $form_new_bucket.attr('action');
                let data = getFormJson($form_new_bucket);
                data.name = input_name;
                return $.ajax({
                    url: url,
                    type: 'post',
                    data: data,
                    timeout: 200000,
                    success: (result) => {
                        if (result.code === 200){
                            return result;
                        }else{
                            swal.showValidationMessage(
                            `Request failed: ${result.error_text}`
                            );
                        }
                    },
                    error: (error) => {
                        swal.showValidationMessage(
                            `Request failed: ${result.error_text}`
                        );
                    },
                    headers: {'X-Requested-With': 'XMLHttpRequest'},//django判断是否是异步请求时需要此响应头
                    clearForm: false,//禁止清除表单
                    resetForm: false //禁止重置表单
                });
            },
            allowOutsideClick: () => !swal.isLoading()
        }).then(
            (result) => {
                if (result.value) {
                    swal({
                        title: `创建存储桶“${result.value.bucket_name}”成功`,
                    }).then(() => {
                        // location.reload(true);// 刷新当前页面
                        location.reload(result.value.redirect_to);// 重定向
                    } )
                }
             },
            (error) => {
                swal(`Request failed:发生错误，创建失败！`);
            }
        )
    }

    //
    // 创建存储桶按钮
    //
    $("#btn-new-bucket").on("click",
        function () {
            // $("#new-bucket-div").show();//
            on_create_bucket();//对话框方式
        }
    );

    //
    // 取消创建存储桶按钮
    //
    $("#btn-cancel").on("click", function () {
        $("#form-new-bucket input[name='name']").val('');
        $('#tip_text').text('');
        $("#new-bucket-div").hide();
    })


    //
    // 全选/全不选
    //
    $(':checkbox[data-check-target]').click(function () {
        let target = $(this).attr('data-check-target');
        let btn_del_bucket = $('#btn-del-bucket');
        if ($(this).prop('checked')) {
            $(target).prop('checked', true); // 全选
            $(target).parents('tr').addClass('danger'); // 选中时添加 背景色类
            if (is_exists_checked()){
                btn_del_bucket.removeClass('disabled');   //鼠标悬停时，使按钮表现为可点击状态
                btn_del_bucket.attr('disabled', false); //激活对应按钮
            }
        } else {
            $(target).prop('checked', false); // 全不选
            $(target).parents('tr').removeClass('danger');// 不选中时移除 背景色类
            btn_del_bucket.addClass('disabled'); //鼠标悬停时，使按钮表现为不可点击状态
            btn_del_bucket.attr('disabled', true);//失能对应按钮
        }
    });

    //
    // 表格中每一行单选checkbox
    //
    $('.item-checkbox').click(function () {
        let btn_del_bucket = $('#btn-del-bucket');
        if ($(this).prop('checked')){
            $(this).parents('tr').addClass('danger');
            btn_del_bucket.removeClass('disabled');
            btn_del_bucket.attr('disabled', false); //激活对应按钮
        }else{
            $(this).parents('tr').removeClass('danger');
            if (!is_exists_checked()){
                btn_del_bucket.addClass('disabled');
                btn_del_bucket.attr('disabled', true); //失能对应按钮
            }
        }
    })


    //
    // 检测是否有选中项
    //
    function is_exists_checked() {
        if ($(".item-checkbox:checked").size() === 0)
            return false;
        else
            return true;
    }

    //
    // 删除存储桶按钮
    //
    $('#btn-del-bucket').click(function () {
        if(!is_exists_checked()){
            alert('请先选择要删除的存储桶');
            return;
        }
        if(!confirm("确认删除选中的存储桶？"))
            return;
        //获取选中的存储桶的id
        var arr = new Array();
        let bucket_list_checked = $("#bucket-table #bucket-list-item :checkbox:checked");
        bucket_list_checked.each(function (i) {
            arr[i] = $(this).val();
        });

        let csrf_code = $(':input[name=csrfmiddlewaretoken]').val();
        if (arr.length > 0){
            $.ajax({
                url: $(this).val(),
                type: 'delete',
                data: {
                    'ids': arr,// 存储桶id数组
                },
                traditional: true,//传递数组时需要设为true
                success: function (data) {
                    if (data.code === 200) {
                        bucket_list_checked.parents('tr').remove();
                        alert('已删除:' + data.code_text);
                    }
                    else
                        alert('删除失败:' + data.error_text);
                },
                error: function (err) {
                    alert('删除失败，' + err.status + ':' + err.statusText);
                },
                headers: {
                    'X-Requested-With': 'XMLHttpRequest',
                    'X-CSRFToken': csrf_code,
                },
            })
        }
    })
})();



