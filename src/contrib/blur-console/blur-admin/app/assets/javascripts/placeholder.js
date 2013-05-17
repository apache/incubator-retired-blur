$(document).ready(function() {
  if (!Modernizr.input.placeholder) {
    $('[placeholder]').focus(function() {
      var input = $(this);
      if (input.val() === input.attr('placeholder')) {
        input.val('');
        input.removeClass('placeholder');
      }
      if (input.attr('placeholder') === 'Password') input[0].type = 'password';
    }).blur(function() {
      var input = $(this);
      if (input.val() === '' || input.val() === input.attr('placeholder')) {
        input.addClass('placeholder');
        input.val(input.attr('placeholder'));
        if (input.attr('placeholder') === 'Password') input[0].type = 'text';
      }
    }).blur();
    $('[placeholder]').parents('form').submit(function() {
      $(this).find('[placeholder]').each(function() {
      	var input = $(this);
      	if (input.val() === input.attr('placeholder')) input.val('');
      });
    });
  }
});