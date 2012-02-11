module HdfsHelper
  FILE_ICONS = {'pdf' => 'pdf.ico', 'rtf' => 'rtf.ico', 'text'=>'text.ico', 'txt'=>'text.ico'}
  DEFAULT_ICON = 'default.ico'

  def file_icon(file)
    if file.isdir
      image_tag 'open_folder.png', :class=>'icon'
    else
      type = file_name_from_path(file.path).split('.').last
      image_tag FILE_ICONS[type]||DEFAULT_ICON, :class=>'icon'
    end
  end
  
  def file_name_from_path(file_name)
    file_name.split('/').last
  end
end