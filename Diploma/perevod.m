function [f1,F,f2,X,Y]=perevod(M,L,H)
for i=1:H
  for j=1:L 
      I=(i-1)*L+j;
      X(i,j)=M(I,4);
      Y(i,j)=M(I,5);
      f1(i,j)=M(I,1); 
      F(i,j)=M(I,2);   
      f2(i,j)=M(I,3);
  end
end
