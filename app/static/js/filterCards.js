// filters comedy specials by topic/category upon changing the dropdown
$(document).ready(function(){
    $(".dropdown-menu a").click(function(){
        // change dropdown to show selected option
        $(".btn-secondary:first-child").text($(this).text());
        $(".btn-secondary:first-child").val($(this).text());

        // filter topics
        var optionValue = $(this).attr("data-option");
        $('div[data-topic-' + optionValue.toString() + ' = "0"]').hide();
        $('div[data-topic-' + optionValue.toString() + ' = "1"]').show();
    });
});