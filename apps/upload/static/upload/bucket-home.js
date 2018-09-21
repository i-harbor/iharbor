$form_new_bucket = $('#form-new-bucket');
$form_new_bucket.on('submit', function (event) {
    event.preventDefault();
    $.ajax({
        url: $form_new_bucket.attr('action'),//'{% url 'upload: bucket_view' %}',
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




