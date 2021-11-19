function M=virezka(M,L,H)
j=1;
for  i=1:2:H-1
           M(i*L+j,:)=[]; 
           j=j+1;
end