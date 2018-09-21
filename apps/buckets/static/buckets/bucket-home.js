$form_new_bucket = $('#form-new-bucket');
$form_new_bucket.on('submit', function (event) {
    event.preventDefault();
    $.ajax({
        url: $form_new_bucket.attr('action'),
        type: 'post',
        data: $form_new_bucket.serialize(),
        success: function (data) {
            if (data.code == 200)
                location.href = data.redirect_to;
            else
                $('#tip_text').text(data.error_text);
        },
        error: function (err) {
            $('#tip_text').text(err.status+':'+err.statusText);
        },
        headers: {'X-Requested-With': 'XMLHttpRequest'},
        clearForm: false,//禁止清除表单
        resetForm: false //禁止重置表单
    });
});

$("#btn-new-bucket").on("click",
    function () {
        $("#new-bucket-div").show();
    }
);


$("#btn-cancel").on("click", function () {
    $('#form-new-bucket :input').val('');
    $('#tip_text').text('');
    $("#new-bucket-div").hide();
})


//
//全选/全不选
//
$(':checkbox[data-check-target]').click(function () {
    var target = $(this).attr('data-check-target');
    if ($(this).prop('checked')) {
        $(target).prop('checked', true); // 全选
        $(target).parents('tr').addClass('danger'); // 选中时添加 背景色类
        $('#btn-del-bucket').removeClass('disabled');   //激活对应按钮
    } else {
        $(target).prop('checked', false); // 全不选
        $(target).parents('tr').removeClass('danger');// 不选中时移除 背景色类
        $('#btn-del-bucket').addClass('disabled'); //失能对应按钮
    }
});

$('.item-checkbox').click(function () {
    if ($(this).prop('checked')){
        $(this).parents('tr').addClass('danger');
        $('#btn-del-bucket').removeClass('disabled');
    }else{
        $(this).parents('tr').removeClass('danger');
        if (!is_exists_checked())
            $('#btn-del-bucket').addClass('disabled');
    }

})

function is_exists_checked() {
    if ($(".item-checkbox:checked").size() == 0)
        return false;
    else
        return true;
}
