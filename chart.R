library('tidyverse')
posts = read_csv('output/cincodenada.csv') %>%
    mutate(
           timestamp = as.POSIXct(date),
           month = as.integer(strftime(date, "%Y")) + as.integer(strftime(date, "%m"))/12,
           reblog_type = factor(is_reblog, c("False","True"), c("Reblog","Original"))
   )

p = ggplot(posts, aes(x=month-1/24, fill=reblog_type)) +
    geom_bar(width=1/12) +
    scale_fill_manual(values=c("#008837","#7b3294")) +
    scale_x_continuous(expand=c(0,0)) +
    labs(title="Joel's Tumblr posts per month, by type", x="", y="Number of posts", fill="Post Type") +
    theme(legend.position='bottom')

png('posts_by_month.png', w=1200,h=600,res=96)
p
dev.off()
