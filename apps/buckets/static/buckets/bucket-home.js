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
            if (data.code == 200)
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
// 创建存储桶按钮
//
$("#btn-new-bucket").on("click",
    function () {
        $("#new-bucket-div").show();
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
    if ($(".item-checkbox:checked").size() == 0)
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
                if (data.code == 200) {
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



