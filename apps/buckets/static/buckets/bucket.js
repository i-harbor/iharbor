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
    if ($(this).prop('checked')){
        $(this).parents('tr').addClass('danger');
    }else{
        $(this).parents('tr').removeClass('danger');
    }
})


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
    $("#div-upload-file").hide();
})
